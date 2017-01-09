{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTSyntax                 #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE RecursiveDo                #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeApplications           #-}

module Node (

      Node(..)
    , nodeEndPointAddress
    , NodeAction(..)
    , node

    , MessageName
    , Message (..)
    , messageName'

    , SendActions(sendTo, withConnectionTo)
    , ConversationActions(send, recv, getCompanion)
    , Worker
    , Listener
    , ListenerAction(..)

    , hoistSendActions
    , hoistConversationActions
    , LL.NodeId(..)

    ) where

import           Control.Monad.Fix          (MonadFix)
import qualified Data.ByteString.Lazy       as LBS
import           Data.List                  (partition)
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as M
import           Data.Proxy                 (Proxy (..))
import           Message.Message            (MessageName)
import           Message.Message
import           Mockable.Channel
import           Mockable.Class
import           Mockable.Concurrent
import           Mockable.Exception
import           Mockable.SharedAtomic
import qualified Network.Transport.Abstract as NT
import           Node.Internal              (ChannelIn (..), ChannelOut (..))
import qualified Node.Internal              as LL
import           System.Random              (StdGen)

data Node m = Node {
      nodeId       :: LL.NodeId
    , nodeEndPoint :: NT.EndPoint m
    }

nodeEndPointAddress :: Node m -> NT.EndPointAddress
nodeEndPointAddress = NT.address . nodeEndPoint

type Worker packing m = SendActions packing m -> m ()

-- TODO: rename all `ListenerAction` -> `Listener`?
type Listener = ListenerAction

data SystemMsg
    = ConnectionOpened LL.NodeId
    | ConnectionClosed LL.NodeId

data ListenerAction packing m where
    -- | A listener that handles a single isolated incoming message
    ListenerActionOneMsg
        :: ( Serializable packing msg, Message msg )
        => (LL.NodeId -> SendActions packing m -> msg -> m ())
        -> ListenerAction packing m

    -- | A listener that handles an incoming bi-directional conversation.
    ListenerActionConversation
        :: ( Packable packing snd, Unpackable packing rcv, Message rcv )
        => (LL.NodeId -> ConversationActions snd rcv m -> m ())
        -> ListenerAction packing m

    -- | Listener, which informs about network related events like connection closure.
    -- It's invoked synchronously with other stuff (e.g. "connection opened" listener
    -- is followed by processing of first message), so consider using quick actions here.
    ListenerActionSystemMsg
        :: (SystemMsg -> m ())
        -> ListenerAction packing m

-- | Gets message type basing on type of incoming messages
listenerMessageName :: Listener packing m -> MessageName
listenerMessageName (ListenerActionOneMsg f) =
    let msgName :: Message msg => (msg -> m ()) -> Proxy msg -> MessageName
        msgName _ = messageName
    in  msgName (f undefined undefined) Proxy
listenerMessageName (ListenerActionConversation f) =
    let msgName :: Message rcv
                => (ConversationActions snd rcv m -> m ())
                -> Proxy rcv
                -> MessageName
        msgName _ = messageName
    in  msgName (f undefined) Proxy
listenerMessageName (ListenerActionSystemMsg _) =
    error "listenerMessageName : no name for ListenerActionSystemMsg"

isSystemListener :: Listener packing m -> Bool
isSystemListener (ListenerActionSystemMsg _) = True
isSystemListener _                           = False

data SendActions packing m = SendActions {
       -- | Send a isolated (sessionless) message to a node
       sendTo :: forall msg .
              ( Packable packing msg, Message msg )
              => LL.NodeId
              -> msg
              -> m (),

       -- | Establish a bi-direction conversation session with a node.
       withConnectionTo
           :: forall snd rcv.
            ( Packable packing snd, Message snd, Unpackable packing rcv )
           => LL.NodeId
           -> (ConversationActions snd rcv m -> m ())
           -> m ()
     }

data ConversationActions body rcv m = ConversationActions {
       -- | Send a message within the context of this conversation
       send         :: body -> m (),

       -- | Receive a message within the context of this conversation.
       --   'Nothing' means end of input (peer ended conversation).
       recv         :: m (Maybe rcv),

       -- | Id of peer node.
       getCompanion :: LL.NodeId
     }

hoistConversationActions :: (forall a. n a -> m a) -> ConversationActions body rcv n -> ConversationActions body rcv m
hoistConversationActions nat ConversationActions {..} =
  ConversationActions send' recv' getCompanion
      where
        send' = nat . send
        recv' = nat recv

hoistSendActions :: (forall a. n a -> m a) -> (forall a. m a -> n a) -> SendActions p n -> SendActions p m
hoistSendActions nat rnat SendActions {..} = SendActions sendTo' withConnectionTo'
  where
    sendTo' nodeId msg = nat $ sendTo nodeId msg
    withConnectionTo' nodeId convActionsH =
        nat $ withConnectionTo nodeId  $ \convActions -> rnat $ convActionsH $ hoistConversationActions nat convActions

-- | Keeps map with processing listeners and system listener
data ListenerIndex packing m = ListenerIndex
    { processingListeners :: Map MessageName (ListenerAction packing m)
    , systemListener      :: ListenerAction packing m
    }

makeListenerIndex :: ( Monad m )
                  => [Listener packing m]
                  -> (ListenerIndex packing m, [MessageName])
makeListenerIndex listeners =
    let (processingListeners, systemListeners) = partition isSystemListener listeners
        (processing, conflictingNames) = foldr combine (M.empty, []) processingListeners
        system = head $ systemListeners ++ [noSystemListener]
    in  (ListenerIndex processing system, conflictingNames)
    where
    combine action (dict, existing) =
        let name = listenerMessageName action
            (replaced, dict') = M.insertLookupWithKey (\_ _ _ -> action) name action dict
            overlapping = maybe [] (const [name]) replaced
        in  (dict', overlapping ++ existing)
    noSystemListener = ListenerActionSystemMsg $ \_ -> return ()

-- | Send actions for a given 'LL.Node'.
nodeSendActions
    :: forall m packing .
       ( Mockable Channel m, Mockable Throw m, Mockable Catch m
       , Mockable Bracket m, Mockable Fork m, Mockable SharedAtomic m
       , Packable packing MessageName )
    => LL.Node m
    -> packing
    -> SendActions packing m
nodeSendActions nodeUnit packing =
    SendActions nodeSendTo nodeWithConnectionTo
  where

    nodeSendTo
        :: forall msg .
           ( Packable packing msg, Message msg )
        => LL.NodeId
        -> msg
        -> m ()
    nodeSendTo = \nodeId msg ->
        LL.withOutChannel nodeUnit nodeId $ \channelOut ->
            LL.writeChannel channelOut $ concatMap LBS.toChunks
                [ packMsg packing $ messageName' msg
                , packMsg packing msg
                ]

    nodeWithConnectionTo
        :: forall snd rcv .
           ( Packable packing snd, Message snd, Unpackable packing rcv )
        => LL.NodeId
        -> (ConversationActions snd rcv m -> m ())
        -> m ()
    nodeWithConnectionTo = \nodeId f ->
        LL.withInOutChannel nodeUnit nodeId $ \inchan outchan -> do
            let msgName  = messageName (Proxy :: Proxy snd)
                cactions :: ConversationActions snd rcv m
                cactions = nodeConversationActions nodeUnit nodeId packing inchan outchan
            LL.writeChannel outchan . LBS.toChunks $
                packMsg packing msgName
            f cactions

-- | Conversation actions for a given peer and in/out channels.
nodeConversationActions
    :: forall packing snd rcv m .
       ( Mockable Throw m, Mockable Bracket m, Mockable Channel m, Mockable SharedAtomic m
       , Packable packing snd
       , Packable packing MessageName
       , Unpackable packing rcv
       )
    => LL.Node m
    -> LL.NodeId
    -> packing
    -> ChannelIn m
    -> ChannelOut m
    -> ConversationActions snd rcv m
nodeConversationActions _ nodeId packing inchan outchan =
    ConversationActions nodeSend nodeRecv nodeId
    where

    nodeSend = \body ->
        LL.writeChannel outchan . LBS.toChunks $ packMsg packing body

    nodeRecv = do
        next <- recvNext' inchan packing
        case next of
            End     -> pure Nothing
            NoParse -> error "Unexpected end of conversation input"
            Input t -> pure (Just t)

data NodeAction packing m t = NodeAction [Listener packing m] (SendActions packing m -> m t)

-- | Spin up a node. You must give a function to create listeners given the
--   'NodeId', and an action to do given the 'NodeId' and sending actions.
--   The node will stop and clean up once that action has completed. If at
--   this time there are any listeners running, they will be allowed to
--   finished.
node
    :: forall packing m t .
       ( Mockable Fork m, Mockable Throw m, Mockable Channel m
       , Mockable SharedAtomic m, Mockable Bracket m, Mockable Catch m
       , Mockable Async m
       , MonadFix m
       , Serializable packing MessageName
       )
    => NT.Transport m
    -> StdGen
    -> packing
    -> (Node m -> m (NodeAction packing m t))
    -> m t
node transport prng packing k = do
    rec { llnode <- LL.startNode transport prng
            (handlerIn listenerIndex sendActions)
            (handlerInOut llnode listenerIndex)
        ; let nId = LL.nodeId llnode
        ; let endPoint = LL.nodeEndPoint llnode
        ; let nodeUnit = Node nId endPoint
        ; NodeAction listeners act <- k nodeUnit
          -- Index the listeners by message name, for faster lookup.
          -- TODO: report conflicting names, or statically eliminate them using
          -- DataKinds and TypeFamilies.
        ; let listenerIndex :: ListenerIndex packing m
              (listenerIndex, _conflictingNames) = makeListenerIndex listeners
        ; let sendActions = nodeSendActions llnode packing
        }
    act sendActions `finally` LL.stopNode llnode
  where
    -- Handle incoming data from unidirectional connections: try to read the
    -- message name, use it to determine a listener, parse the body, then
    -- run the listener.
    handlerIn :: ListenerIndex packing m -> SendActions packing m -> LL.NodeId -> ChannelIn m -> m ()
    handlerIn listenerIndex sendActions peerId inchan =
        invokeSystemMessages listenerIndex peerId $ do
            input <- recvNext' inchan packing
            case input of
                End -> error "handerIn : unexpected end of input"
                -- TBD recurse and continue handling even after a no parse?
                NoParse -> error "handlerIn : failed to parse message name"
                Input msgName -> do
                    let listener = M.lookup msgName (processingListeners listenerIndex)
                    case listener of
                        Just (ListenerActionOneMsg action) -> do
                            input' <- recvNext' inchan packing
                            case input' of
                                End -> error "handerIn : unexpected end of input"
                                NoParse ->error "handlerIn : failed to parse message body"
                                Input msgBody -> action peerId sendActions msgBody
                        -- If it's a conversation listener, then that's an error, no?
                        Just (ListenerActionConversation _) -> error ("handlerIn : wrong listener type. Expected unidirectional for " ++ show msgName)
                        Just (ListenerActionSystemMsg _) -> error "handlerIn : unexpected system listener"
                        Nothing -> error ("handlerIn : no listener for " ++ show msgName)

    -- Handle incoming data from a bidirectional connection: try to read the
    -- message name, then choose a listener and fork a thread to run it.
    handlerInOut :: LL.Node m
                 -> ListenerIndex packing m
                 -> LL.NodeId
                 -> ChannelIn m
                 -> ChannelOut m
                 -> m ()
    handlerInOut nodeUnit listenerIndex peerId inchan outchan =
        invokeSystemMessages listenerIndex peerId $ do
            input <- recvNext' inchan packing
            case input of
                End -> error "handlerInOut : unexpected end of input"
                NoParse -> error "handlerInOut : failed to parse message name"
                Input msgName -> do
                    let listener = M.lookup msgName (processingListeners listenerIndex)
                    case listener of
                        Just (ListenerActionConversation action) ->
                            let cactions = nodeConversationActions nodeUnit peerId packing
                                    inchan outchan
                            in  action peerId cactions
                        Just (ListenerActionOneMsg _) -> error ("handlerInOut : wrong listener type. Expected bidirectional for " ++ show msgName)
                        Just (ListenerActionSystemMsg _) -> error "handlerIn : unexpected system listener"
                        Nothing -> error ("handlerInOut : no listener for " ++ show msgName)

    invokeSystemMessages listenerIndex peerId =
        let ListenerActionSystemMsg listener = systemListener listenerIndex
        in  bracket_ (listener $ ConnectionOpened peerId)
                     (listener $ ConnectionClosed peerId)

recvNext'
    :: ( Mockable Channel m, Unpackable packing thing )
    => ChannelIn m
    -> packing
    -> m (Input thing)
recvNext' (ChannelIn chan) packing = unpackMsg packing chan
