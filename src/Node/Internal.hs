{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE GADTSyntax #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Node.Internal (
    NodeId(..),
    Node(..),
    ChannelIn(..),
    ChannelOut(..),
    startNode,
    stopNode,
    withOutChannel,
    withInOutChannel,
    writeChannel,
    readChannel,

    Statistics(..),

    Policy,
    DispatchPolicy,
    dispatchPolicy,
    noDelayPolicy,
    uniformDelayPolicy,
    policyCurrentTime,
    policyConnectionStates,
    policyStatistics
  ) where

import Data.Binary     as Bin
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString      as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Builder.Extra as BS
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Monoid
import Data.Typeable
import Data.List (foldl')
import Data.Functor.Identity (Identity(runIdentity))
import Control.Applicative (Alternative)
import Control.Monad (mzero, MonadPlus)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT(runReaderT))
import qualified Control.Monad.Trans.Reader as Reader
import Control.Monad.Trans.State.Strict (StateT(runStateT))
import qualified Control.Monad.Trans.State.Strict as State
import Control.Monad.Trans.Maybe (MaybeT(runMaybeT))
import Control.Exception hiding (bracket, throw, catch, finally)
import qualified Network.Transport.Abstract as NT
import qualified Network.Transport as NTT (EventErrorCode(..))
import System.Random (StdGen, random, randomR, Random)
import Mockable.Class
import Mockable.Concurrent
import Mockable.Exception
import Mockable.Time
import qualified Mockable.Channel as Channel
import Mockable.SharedAtomic

-- | A 'NodeId' wraps a network-transport endpoint address
newtype NodeId = NodeId NT.EndPointAddress
  deriving (Eq, Ord, Show)

-- | The state of a Node, to be held in a shared atomic cell because other
--   threads will mutate it in order to set up bidirectional connections.
data NodeState m = NodeState {
      nodeStateGen :: !StdGen
      -- ^ To generate nonces.
    , nodeStateNonces :: !(Map Nonce (NonceState m))
      -- ^ Nonces identify bidirectional connections, and this gives the state
      --   of each one.
    , nodeStateFinished :: ![(Either NT.ConnectionId Nonce, Maybe SomeException)]
      -- ^ Connection identifiers or nonces for handlers which have finished.
      --   'Nonce's for bidirectional connections, 'ConnectionId's for handlers
      --   spawned to respond to incoming connections.
    }

-- | A 'Node' is a network-transport 'EndPoint' with bidirectional connection
--   state and a thread to dispatch network-transport events.
data Node (m :: * -> *) = Node {
       nodeEndPoint         :: NT.EndPoint m,
       nodeDispatcherThread :: ThreadId m,
       nodeState :: SharedAtomicT m (NodeState m)
     }

-- | Used to identify bidirectional connections.
newtype Nonce = Nonce {
      getNonce :: Word64
    }

deriving instance Show Nonce
deriving instance Eq Nonce
deriving instance Ord Nonce
deriving instance Random Nonce
deriving instance Binary Nonce

data NodeException =
       ProtocolError String
     | InternalError String
  deriving (Show, Typeable)

instance Exception NodeException

-- | Input from the wire. Nothing means there's no more to come.
newtype ChannelIn m = ChannelIn (Channel.ChannelT m (Maybe BS.ByteString))

channelInWrite
    :: ( Mockable Channel.Channel m )
    => ChannelIn m
    -> [BS.ByteString]
    -> m Int
channelInWrite (ChannelIn chan) = go 0
    where
    go !n [] = pure n
    go !n (bs : bss) = do
        _ <- Channel.writeChannel chan (Just bs)
        go (BS.length bs + n) bss

-- | Output to the wire.
newtype ChannelOut m = ChannelOut (NT.Connection m)

-- | Bring up a 'Node' using a network transport.
startNode :: ( Mockable SharedAtomic m, Mockable Fork m, Mockable Bracket m
             , Mockable Channel.Channel m, Mockable Throw m, Mockable Catch m
             , HasTime m, Mockable GetCurrentTime m, Mockable Delay m )
          => NT.EndPoint m
          -> StdGen
          -> DispatchPolicy m
          -> (NodeId -> ChannelIn m -> m ())
          -- ^ Handle incoming unidirectional connections.
          -> (NodeId -> ChannelIn m -> ChannelOut m -> m ())
          -- ^ Handle incoming bidirectional connections.
          -> m (Node m)
startNode endPoint prng policy handlerIn handlerOut = do
    sharedState <- newSharedAtomic (NodeState prng Map.empty [])
    tid <- fork $
        nodeDispatcher endPoint sharedState policy handlerIn handlerOut
    --TODO: exceptions in the forkIO
    --they should be raised in this thread.
    return Node {
      nodeEndPoint         = endPoint,
      nodeDispatcherThread = tid,
      nodeState            = sharedState
    }

-- | Stop a 'Node', closing its network transport endpoint.
stopNode :: Node m -> m ()
stopNode Node {..} =
    NT.closeEndPoint nodeEndPoint
    -- This eventually will shut down the dispatcher thread, which in turn
    -- ought to stop the connection handling threads.
    -- It'll also close all TCP connections.

-- | State which is local to the dispatcher, and need not be accessible to
--   any other thread.
data DispatcherState m = DispatcherState {
      dispatcherConnectionStates :: !(Map NT.ConnectionId (ConnectionState m))
    , dispatcherStatistics :: !(Statistics m)
    }

-- | A bundle of statistics to be gathered by the dispatcher and used to inform
--   a policy of backpressure.
data Statistics (m :: * -> *) = Statistics {
      statsTotalHandlers :: !Integer
    , statsCompletedHandlers :: !Integer
    , statsFailedHandlers :: !Integer
    , statsBytesReceived :: !Integer
    }

initialStatistics :: Statistics m
initialStatistics = Statistics {
      statsTotalHandlers = 0
    , statsCompletedHandlers = 0
    , statsFailedHandlers = 0
    , statsBytesReceived = 0
    }

-- | Derive the number of active handlers from a Statistics value.
statsActiveHandlers :: Statistics m -> Integer
statsActiveHandlers stats = total - (completed + failed)
    where
    total = statsTotalHandlers stats
    completed = statsCompletedHandlers stats
    failed = statsFailedHandlers stats

-- TODO:
-- we shall want to run the policy *after* an event, and inform the policy
-- computation of the event, i.e. whether it was
--   - received
--   - opened
--   - closed
--   - error
-- We should give the statistics as they stood before the request was answered,
-- and in the case of received we'll also give the number of bytes received.
--
-- Would like for the policy to be able to determine, for instance, whether a
-- new connection should be accepted or "dropped" (i.e. forgotten, so later
-- data is immediately garbage collected).
-- Oh no, we can't ignore new connections unless we deliver an error to the
-- sender, for we want ordering. That's unless we give the ordering guarantee
-- only within an established connection (so bidirectional connections are
-- ordered).
--
-- Anyway, let's focus on what defensive policies we can actually implement
-- now: i.e. delaying a receive. We can make a test in which many nodes harass
-- a given node, and see whether the victim's memory usage remains bounded.
-- What would such a policy look like? Some function of the number of active
-- handlers?
-- How about bytes per second input? We can compute the mean and maybe the
-- standard deviation incrementally over all time, and we could also compute
-- these over another window, say, the last n seconds. If we see that the last
-- n seconds has a much higher mean and rather low standard deviation, we'll
-- slow down. Slowing down will certainly lower the mean (maybe increase std.
-- dev.) so the next iteration may act differently.
-- - We *can* incrementally compute bytes / seconds
-- - std. dev. doesn't really make sense, though, does it? n here is not the
--   number of samples, it's number of seconds, and we only sample at random
--   points (when an event is received).
-- Ok so we treat each sample as bytes / seconds, i.e. the size of data
-- received over the time since the last receive. Now we do have n as number of
-- samples. Yeah, that's definitely what we want. 
-- This can all be implemented as a special policy with no extra support from
-- the dispatcher. It can be inferred from special state (parameter of the
-- DispatchPolicy), absolute time parameter, and the StatisticsEvent
-- (SEDataReceived).
-- So we just have to pick a desired bytes/second and then the policy will
-- adjust itself so it never exceeds that value. Delay by the amount of time
-- such that it would cause the actual bytes/second to reach the desired, if
-- a given number of bytes is received. Ah, so we also want to know stats about
-- the number of bytes received, not just the rate.
--
-- Really though this is all silly because it still allows DoS attacks. All
-- the attacker must do is connect and repeatedly send massive data. The
-- victim will slow down and everybody will suffer. A good policy will slow
-- down receiving only from the attacker's EndPoint, and probably reject new
-- connection from it as well. These are two things just not possible in
-- network-transport as it stands: rejecting new connections from
-- an EndPoint, and delaying socket reads from an EndPoint.
--
-- So how to implement it? network-transport-tcp has one shared event queue,
-- fed by the multiplexed tcp connections. It therefore does not make sense
-- to add an extra parameter to 'receive'. Instead, we'd have some imperative
-- style thing: 'delayFrom :: EndPoint -> EndPointAddress -> Int -> IO (IO ())'
-- and 'rejectFrom :: EndPoint -> EndPointAddress -> IO (IO ())' where the
-- 'IO's returned will unblock and unreject.
--
-- Doesn't make any sense really... network-transport isn't designed for this.
-- We can't reject a request based on host because network-transport doesn't
-- work like that. It's a tcp-specific thing. But then again that seems right.
-- We wouldn't want to reject any incoming connection if using in-memory because
-- it wouldn't make a difference, the attacker and victim have the same memory
-- space. What we could do is have network-transport-tcp expose a host-based
-- blacklist and traffic shaping api, and then have the policy depend upon the
-- transport.

-- | Events to inform dispatcher policy decisions. Closely related to
--   network-transport events and connection states.
data StatisticsEvent =

      -- | Identifies the connection and the number of bytes received.
      SEDataReceived !NT.ConnectionId !Int

      -- | A new connection opened by a given peer address.
    | SEConnectionOpened !NT.ConnectionId !NT.EndPointAddress

      -- | A closed connection.
    | SEConnectionClosed !NT.ConnectionId

      -- | Something went wrong.
    | SEErrorEvent !(NT.TransportError NT.EventErrorCode)

sizeOfChunks :: ( Integral i ) => [BS.ByteString] -> i
sizeOfChunks = fromIntegral . foldl' (\i bs -> i + BS.length bs) 0

--
--   TODO: will want more capabilities than to just delay all input.
--   Should be able to selectively delay input from a given end point address
--   or a given connection identifier. network-transport doesn't support that
--   though.
data DispatchPolicy (m :: * -> *) = forall s . DispatchPolicy {
      policyState :: s
    , policyTransition :: Policy s m (TimeDelta m)
    }

runDispatchPolicy
    :: StatisticsEvent
    -> DispatcherState m
    -> TimeAbsolute m
    -> DispatchPolicy m
    -> (Maybe (TimeDelta m), DispatchPolicy m)
runDispatchPolicy sevent state time (DispatchPolicy s policy) = (x, DispatchPolicy s' policy)
    where
    (x, s') = runPolicy s sevent state time policy

-- | Give initial state and a policy to get a complete dispatch policy.
dispatchPolicy :: s -> Policy s m (TimeDelta m) -> DispatchPolicy m
dispatchPolicy = DispatchPolicy

newtype Policy (s :: *) (m :: * -> *) (t :: *) = Policy {
      unPolicy :: MaybeT (StateT s (ReaderT (StatisticsEvent, DispatcherState m, TimeAbsolute m) Identity)) t
    }

deriving instance Functor (Policy s m)
deriving instance Applicative (Policy s m)
deriving instance Monad (Policy s m)
deriving instance Alternative (Policy s m)
deriving instance MonadPlus (Policy s m)

runPolicy
    :: s
    -> StatisticsEvent
    -> DispatcherState m
    -> TimeAbsolute m
    -> Policy s m t
    -> (Maybe t, s)
runPolicy s sevent state time p =
    runIdentity $
    flip runReaderT (sevent, state, time) $
    flip runStateT s $
    runMaybeT $
    unPolicy $ p

getPolicyState :: Policy s m s
getPolicyState = Policy . lift $ State.get

putPolicyState :: s -> Policy s m ()
putPolicyState = Policy . lift . State.put

policyCurrentTime :: Policy s m (TimeAbsolute m)
policyCurrentTime = Policy . fmap trd . lift . lift $ Reader.ask
  where
  trd (_,_,x) = x

policyEvent :: Policy s m StatisticsEvent
policyEvent = Policy . fmap fst . lift . lift $ Reader.ask
  where
  fst (x,_,_) = x

policyDispatcherState :: Policy s m (DispatcherState m)
policyDispatcherState = Policy . fmap snd . lift . lift $ Reader.ask
  where
  snd (_,x,_) = x

policyStatistics :: Policy s m (Statistics m)
policyStatistics = fmap dispatcherStatistics policyDispatcherState

policyConnectionStates :: Policy s m (Map NT.ConnectionId (ConnectionState m))
policyConnectionStates = fmap dispatcherConnectionStates policyDispatcherState

-- | Never delay.
noDelayPolicy :: DispatchPolicy m
noDelayPolicy = dispatchPolicy () mzero

-- | Always delay by a given fraction of seconds.
uniformDelayPolicy :: ( HasTime m, Real a ) => a -> DispatchPolicy m
uniformDelayPolicy n = dispatchPolicy () (pure (realToFrac n))

{-
-- | Randomly delay within the given bounds (lo, hi), as fractions of seconds.
--   All values in the range are equally likely.
rangeDelayPolicy :: ( HasTime m, RealFrac a ) => (a, a) -> DispatchPolicy m
rangeDelayPolicy (lo, hi) = randomDelayPolicy distr
  where
  distr t = (realToFrac t) * hi + (realToFrac (1 - t)) * lo

-- | Randomly delay according to some distribution function [0,1] -> a
--   as fractions of seconds.
--   All input values (in [0,1]) are equally likely.
randomDelayPolicy :: ( HasTime m, Real a ) => (Double -> a) -> DispatchPolicy m
randomDelayPolicy distr = do
    t <- policyRandomValue (0.0, 1.0)
    uniformDelayPolicy (distr t)
-}

-- | The state of a connection (associated with a 'ConnectionId', see
--   'DispatcherState'.
data ConnectionState m =

       -- | We got a new connection and are waiting on the first chunk of data
      ConnectionNew !NodeId

      -- | We got the first chunk of data, we're now waiting either for more
      --   data or for the connection to be closed. This supports the small
      --   message optimisation.
    | ConnectionNewChunks !NodeId ![BS.ByteString]

      -- | We've forked a thread to handle the message. The connection is still
      --   open and data is still arriving. We have a channel to pass the
      --   incoming chunks off to the other thread.
    | ConnectionReceiving !(ThreadId m) !(Channel.ChannelT m (Maybe BS.ByteString))

      -- | We've forked a thread to handle the message. The connection is now
      --   closed and we have all the data already, but the thread we forked
      --   to handle it is still active.
    | ConnectionClosed !(ThreadId m)

      -- | The handler which we forked to process the data has finished.
      --   Subsequent incoming data has nowhere to go.
    | ConnectionHandlerFinished !(Maybe SomeException)

instance Show (ConnectionState m) where
    show term = case term of
        ConnectionNew nodeid -> "ConnectionNew " ++ show nodeid
        ConnectionNewChunks nodeid _ -> "ConnectionNewChunks " ++ show nodeid
        ConnectionReceiving _ _ -> "ConnectionReceiving"
        ConnectionClosed _ -> "ConnectionClosed"
        ConnectionHandlerFinished e -> "ConnectionHandlerFinished " ++ show e

-- | Bidirectional connections (conversations) are identified not by
--   'ConnectionId' but by 'Nonce', because their handlers run before any
--   connection is established.
--
--   Once a connection for a conversation is established, its nonce is
--   associated with it via 'NonceHandlerConnected', but prior to this it's
--   'NonceHandlerNotConnected', meaning the handler is running but a
--   SYN/ACK handshake has not been completed.
--
--   The NonceState must be accessible by the dispatcher and by other threads,
--   because other threads may initiate a conversation.
data NonceState m =

      NonceHandlerNotConnected !(ThreadId m) !(ChannelIn m)

    | NonceHandlerConnected !NT.ConnectionId

      -- TODO use this one. It demands timing out the bidirectional handlers
      -- so that the dispatcher state doesn't grow without bound (in case for
      -- instance an ACK never comes).
    | NonceHandlerFinished (Maybe SomeException)

instance Show (NonceState m) where
    show term = case term of
        NonceHandlerNotConnected _ _ -> "NonceHandlerNotConnected"
        NonceHandlerConnected connid -> "NonceHandlerConnected " ++ show connid

--TODO: extend this to keep track of the number of active threads and total
-- amount of in flight incoming data. This will be needed to inform the
-- back-pressure policy.

-- | The one thread that handles /all/ incoming messages and dispatches them
-- to various handlers.
nodeDispatcher :: forall m .
                  ( Mockable SharedAtomic m, Mockable Fork m, Mockable Bracket m
                  , Mockable Channel.Channel m, Mockable Throw m
                  , Mockable Catch m, Mockable Delay m
                  , Mockable GetCurrentTime m, HasTime m )
               => NT.EndPoint m
               -> SharedAtomicT m (NodeState m)
               -- ^ Nonce states and a StdGen to generate nonces. It's in a
               --   shared atomic because other threads must be able to alter
               --   it when they start a conversation.
               --   The third element of the triple will be updated by handler
               --   threads when they finish.
               -> DispatchPolicy m
               -> (NodeId -> ChannelIn m -> m ())
               -> (NodeId -> ChannelIn m -> ChannelOut m -> m ())
               -> m ()
nodeDispatcher endpoint nodeState dispatcherPolicy handlerIn handlerInOut =
    dispatch (DispatcherState Map.empty initialStatistics) dispatcherPolicy
  where

    finally :: m t -> m () -> m t
    finally action after = bracket (pure ()) (const after) (const action)

    -- Take the dead threads from the shared atomic and release them all from
    -- the map if their connection is also closed.
    -- Only if the connection is closed *and* the handler is finished, can we
    -- forget about a connection id. It could be that the handler is finished,
    -- but we receive more data, in which case we want to somehow make the
    -- peer stop pushing any data.
    -- If the connection is closed after the handler finishes, then the entry
    -- in the state map will be cleared by the dispatcher loop.
    --
    -- TBD use the reported exceptions to inform the dispatcher somehow?
    -- If a lot of threads are giving exceptions, should we change dispatcher
    -- behavior?
    updateStateForFinishedHandlers :: DispatcherState m -> m (DispatcherState m)
    updateStateForFinishedHandlers state = modifySharedAtomic nodeState $ \(NodeState prng nonces finished) -> do
        -- For every Left (c :: NT.ConnectionId) we can remove it from the map
        -- if its connection is closed, or indicate that the handler is finished
        -- so that it will be removed when the connection is closed.
        --
        -- For every Right (n :: Nonce) we act depending on the nonce state.
        -- If the handler isn't connected, we'll just drop the nonce state
        -- entry.
        -- If the handler is connected, we'll update the connection state for
        -- that ConnectionId to say the handler has finished.
        let (state', nonces') = foldl' folder (state, nonces) finished
        --() <- trace ("DEBUG: state map size is " ++ show (Map.size state')) (pure ())
        --() <- trace ("DEBUG: nonce map size is " ++ show (Map.size nonces')) (pure ())
        pure ((NodeState prng nonces' []), state')
        where

        -- Updates connection and nonce states in a left fold.
        folder (dispatcherState, nonces) (connidOrNonce, e) = case connidOrNonce of
            Left connid ->
                let connectionStates = dispatcherConnectionStates dispatcherState
                    connectionStates' = Map.update (connUpdater e) connid connectionStates
                    dispatcherState' = dispatcherState {
                          dispatcherConnectionStates = connectionStates'
                        }
                in  (dispatcherState', nonces)
            Right nonce -> folderNonce (dispatcherState, nonces) (nonce, e)

        folderNonce (dispatcherState, nonces) (nonce, e) = case Map.lookup nonce nonces of
            Nothing -> error "Handler for unknown nonce finished"
            Just (NonceHandlerNotConnected _ _) ->
                (dispatcherState, Map.delete nonce nonces)
            Just (NonceHandlerConnected connid) ->
                let connectionStates = dispatcherConnectionStates dispatcherState
                    connectionStates' = Map.update (connUpdater e) connid connectionStates
                    dispatcherState' = dispatcherState {
                          dispatcherConnectionStates = connectionStates'
                        }
                    nonces' = Map.delete nonce nonces
                in  (dispatcherState', nonces')

        connUpdater :: Maybe SomeException -> ConnectionState m -> Maybe (ConnectionState m)
        connUpdater e connState = case connState of
            ConnectionClosed _ -> Nothing
            _ -> Just (ConnectionHandlerFinished e)

    -- Handle the first chunks received, interpreting the control byte(s).
    handleFirstChunks
        :: NT.ConnectionId
        -> NodeId
        -> [BS.ByteString]
        -> DispatcherState m
        -> m (DispatcherState m)
    handleFirstChunks connid peer@(NodeId peerEndpointAddr) chunks !state =
        case LBS.uncons (LBS.fromChunks chunks) of
            -- Empty. Wait for more data.
            Nothing -> pure state
            Just (w, ws)
                -- Peer wants a unidirectional (peer -> local)
                -- connection. Make a channel for the incoming
                -- data and fork a thread to consume it.
                | w == controlHeaderCodeUnidirectional -> do
                  chan <- Channel.newChannel
                  bytes <- channelInWrite (ChannelIn chan) (LBS.toChunks ws)
                  tid  <- fork $ finishHandler nodeState (Left connid) (handlerIn peer (ChannelIn chan))
                  let stats = dispatcherStatistics state
                      stats' = stats {
                            statsTotalHandlers = statsTotalHandlers stats + 1
                          , statsBytesReceived = statsBytesReceived stats + fromIntegral bytes
                          }
                      connStates = dispatcherConnectionStates state
                      connStates' = Map.insert connid (ConnectionReceiving tid chan) connStates
                      state' = state {
                            dispatcherConnectionStates = connStates'
                          , dispatcherStatistics = stats'
                          }
                  pure state'

                -- Bidirectional header without the nonce
                -- attached. Wait for the nonce.
                | w == controlHeaderCodeBidirectionalAck ||
                  w == controlHeaderCodeBidirectionalSyn
                , LBS.length ws < 8 -> do -- need more data
                  let stats = dispatcherStatistics state
                      connStates = dispatcherConnectionStates state
                      connStates' = Map.insert connid (ConnectionNewChunks peer chunks) connStates
                      state' = state {
                            dispatcherConnectionStates = connStates'
                          , dispatcherStatistics = stats
                          }
                  pure state'

                -- Peer wants a bidirectional connection.
                -- Fork a thread to reply with an ACK and then
                -- handle according to handlerInOut.
                | w == controlHeaderCodeBidirectionalSyn
                , Right (ws',_,nonce) <- decodeOrFail ws -> do
                  chan <- Channel.newChannel
                  mapM_ (Channel.writeChannel chan . Just) (LBS.toChunks ws')
                  let action = do
                          mconn <- NT.connect
                                     endpoint
                                     peerEndpointAddr
                                     NT.ReliableOrdered
                                     NT.ConnectHints{ connectTimeout = Nothing } --TODO use timeout
                          case mconn of
                            Left  err  -> throw err
                            Right conn -> do
                              -- TODO: error handling
                              NT.send conn [controlHeaderBidirectionalAck nonce]
                              handlerInOut peer (ChannelIn chan) (ChannelOut conn)
                                  `finally`
                                  closeChannel (ChannelOut conn)
                  tid <- fork $ finishHandler nodeState (Left connid) action
                  let stats = dispatcherStatistics state
                      stats' = stats {
                            statsTotalHandlers = statsTotalHandlers stats + 1
                          }
                      connStates = dispatcherConnectionStates state
                      connStates' = Map.insert connid (ConnectionReceiving tid chan) connStates
                      state' = state {
                            dispatcherConnectionStates = connStates'
                          , dispatcherStatistics = stats'
                          }
                  pure state'

                -- We want a bidirectional connection and the
                -- peer has acknowledged. Check that their nonce
                -- matches what we sent and if so start writing
                -- to the channel associated with that nonce.
                -- See connectInOutChannel/withInOutChannel for the other half
                -- of the story (where we send SYNs and record
                -- nonces).
                --
                -- A call to withInOutChannel ensures that when the action using
                -- the in/out channel completes, it will update the shared
                -- atomic 'finished' variable to indicate that thread
                -- corresponding to the *nonce* is completed. It may have
                -- already completed at this point, which is weird but not
                -- out of the question (the handler didn't ask to receive
                -- anything). 
                --
                | w == controlHeaderCodeBidirectionalAck
                , Right (ws',_,nonce) <- decodeOrFail ws -> do
                  (tid, ChannelIn chan) <- modifySharedAtomic nodeState $ \(NodeState prng expected finished) ->
                      case Map.lookup nonce expected of
                          -- The nonce is not known. It could be that:
                          -- 1. We really didn't send a SYN for this nonce,
                          --    in which case it's a protocol error.
                          -- 2. We did send a SYN for this nonce, but our
                          --    handler finished before this ACK arrived,
                          --    in which case it's not a protocl error (but
                          --    perhaps a silly use of a bidirectional
                          --    connection).
                          --
                          Nothing -> throw (ProtocolError $ "unexpected ack nonce " ++ show nonce)
                          Just (NonceHandlerNotConnected tid inchan) -> do
                              let !expected' = Map.insert nonce (NonceHandlerConnected connid) expected
                              return ((NodeState prng expected' finished), (tid, inchan))
                          Just _ -> throw (InternalError $ "duplicate or delayed ACK for " ++ show nonce)
                  mapM_ (Channel.writeChannel chan . Just) (LBS.toChunks ws')
                  let stats = dispatcherStatistics state
                      connStates = dispatcherConnectionStates state
                      connStates' = Map.insert connid (ConnectionReceiving tid chan) connStates
                      state' = state {
                            dispatcherConnectionStates = connStates'
                          , dispatcherStatistics = stats
                          }
                  pure state'

                | otherwise ->
                    throw (ProtocolError $ "unexpected control header " ++ show w)

    -- | Use the policy to determine a delay and then loop back into the
    --   dispatcher.
    loop
        :: StatisticsEvent
        -> DispatcherState m
        -> DispatchPolicy m
        -> TimeAbsolute m
        -> m ()
    loop !event !state !dispatchPolicy !timeNow = do
        let (!maybeDelay, !dispatchPolicy') = runDispatchPolicy event state timeNow dispatchPolicy
        -- Multiply by 10^6 because the whole number part of a TimeDelta is to
        -- be interpreted as seconds, but a delay is to be given microseconds.
        _ <- maybe (pure ()) (delay . round . (*) 1000000) maybeDelay
        dispatch state dispatchPolicy'

    dispatch :: DispatcherState m -> DispatchPolicy m -> m ()
    dispatch !state !policy = do
      !timeNow <- getCurrentTime
      !state <- updateStateForFinishedHandlers state
      let connStates = dispatcherConnectionStates state
          stats = dispatcherStatistics state
          continue = \sevent state -> loop sevent state policy timeNow
      event <- NT.receive endpoint
      case event of

          NT.ConnectionOpened connid NT.ReliableOrdered peer ->
              -- Just keep track of the new connection, nothing else
              let state' = state {
                        dispatcherConnectionStates = Map.insert connid (ConnectionNew (NodeId peer)) connStates
                      }
                  sevent = SEConnectionOpened connid peer
              in  continue sevent state'

          NT.ConnectionOpened _ _ _ ->
              throw (ProtocolError "unexpected connection reliability")

          -- receiving data for an existing connection (ie a multi-chunk message)
          NT.Received connid chunks -> do
              let !bytesReceived = sizeOfChunks chunks
                  !stats' = stats {
                        statsBytesReceived = statsBytesReceived stats + bytesReceived
                      }

              case Map.lookup connid connStates of

                  -- TODO: may not be an error. What if we want to ignore a new
                  -- connection, leaving it out of the connection state map so
                  -- that whenever we receive from it, we just forget about the
                  -- data?
                  Nothing ->
                      throw (InternalError "received data on unknown connection")

                  -- TODO: need policy here on queue size
                  Just (ConnectionNew peer) ->
                      let state' = DispatcherState
                              (Map.insert connid (ConnectionNewChunks peer chunks) connStates)
                              stats
                          sevent = SEDataReceived connid (sizeOfChunks chunks)
                      in  continue sevent state'

                  Just (ConnectionNewChunks peer@(NodeId _) chunks0) -> do
                      let allchunks = chunks0 ++ chunks
                      let sevent = SEDataReceived connid (sizeOfChunks allchunks)
                      !state' <- handleFirstChunks connid peer allchunks state
                      continue sevent state'

                  -- Connection is receiving data and there's some handler
                  -- at 'tid' to run it. Dump the new data to its ChannelIn.
                  Just (ConnectionReceiving tid chan) -> do
                      size <- channelInWrite (ChannelIn chan) chunks
                      let sevent = SEDataReceived connid size
                      -- TODO update statistics.
                      continue sevent state

                  Just (ConnectionClosed tid) ->
                      throw (InternalError "received data on closed connection")

                  -- The peer keeps pushing data but our handler is finished.
                  -- What to do? Would like to close the connection but I'm
                  -- not sure that's possible in network-transport. Would like
                  -- to say "stop receiving on this ConnectionId".
                  -- We could maintain an association between ConnectionId and
                  -- EndPointId of the peer, and then maybe patch
                  -- network-transport to allow for selective closing of peer
                  -- connection based on EndPointAddress.
                  Just (ConnectionHandlerFinished maybeException) ->
                      throw (InternalError "received too much data")

          NT.ConnectionClosed connid -> do
              let sevent = SEConnectionClosed connid
              case Map.lookup connid connStates of
                  Nothing ->
                      throw (InternalError "closed unknown connection")

                  -- Connection closed, handler already finished. We're done
                  -- with the connection. The case in which the handler finishes
                  -- *after* the connection closes is taken care of in
                  -- 'updateStateForFinishedHandlers'.
                  Just (ConnectionHandlerFinished _) -> do
                      let connStates = dispatcherConnectionStates state
                          stats = dispatcherStatistics state
                          connStates' = Map.delete connid connStates
                          stats' = stats {
                                statsCompletedHandlers = statsCompletedHandlers stats + 1
                              }
                          state' = state {
                                dispatcherConnectionStates = connStates'
                              , dispatcherStatistics = stats'
                              }
                      continue sevent state'

                  -- Empty message
                  Just (ConnectionNew peer) -> do
                      chan <- Channel.newChannel
                      Channel.writeChannel chan Nothing
                      tid  <- fork $ finishHandler nodeState (Left connid) (handlerIn peer (ChannelIn chan))
                      let connStates' = Map.insert connid (ConnectionClosed tid) connStates
                          state' = state {
                                dispatcherConnectionStates = connStates'
                              }
                      continue sevent state'

                  -- Small message
                  Just (ConnectionNewChunks peer chunks0) -> do
                      !state' <- handleFirstChunks connid peer chunks0 state
                      let connStates = dispatcherConnectionStates state'
                      case Map.lookup connid connStates of
                          Just (ConnectionReceiving tid chan) -> do
                              -- Write Nothing to indicate end of input.
                              _ <- Channel.writeChannel chan Nothing
                              let connStates' = Map.insert connid (ConnectionClosed tid) connStates
                                  state'' = state' {
                                        dispatcherConnectionStates = connStates'
                                      }
                                  sevent = SEConnectionClosed connid
                              continue sevent state''
                          _ -> throw (InternalError "malformed small message")

                  -- End of incoming data. Signal that by writing 'Nothing'
                  -- to the ChannelIn.
                  Just (ConnectionReceiving tid chan) -> do
                      _ <- Channel.writeChannel chan Nothing
                      let sevent = SEConnectionClosed connid
                          connStates' = Map.insert connid (ConnectionClosed tid) connStates
                          state' = state {
                                dispatcherConnectionStates = connStates'
                              }
                      continue sevent state'

                  Just (ConnectionClosed tid) ->
                      throw (InternalError "closed a closed connection")

          NT.EndPointClosed ->
              -- TODO: decide what to do with all active handlers
              -- Throw them a special exception?
              return ()

          NT.ErrorEvent (NT.TransportError (NT.EventErrorCode (NTT.EventConnectionLost peer)) _msg) ->
              throw (InternalError "Connection lost")

          NT.ErrorEvent (NT.TransportError (NT.EventErrorCode NTT.EventEndPointFailed)  msg) ->
              throw (InternalError "EndPoint failed")

          NT.ErrorEvent (NT.TransportError (NT.EventErrorCode NTT.EventTransportFailed) msg) ->
              throw (InternalError "Transport failed")

          NT.ErrorEvent (NT.TransportError NT.UnsupportedEvent msg) ->
              throw (InternalError "Unsupported event")

-- | Augment some m term so that it always updates a 'NodeState' mutable
--   cell when finished, along with the exception if one was raised. We catch
--   all exceptions in order to do this, but they are re-thrown.
--   Use an 'NT.ConnectionId' if the handler is spawned in response to a
--   connection, or a 'Nonce' if the handler is spawned for a locally
--   initiated bidirectional connection.
finishHandler
    :: forall m t .
       ( Mockable SharedAtomic m, Mockable Throw m, Mockable Catch m )
    => SharedAtomicT m (NodeState m)
    -> Either NT.ConnectionId Nonce
    -> m t
    -> m t
finishHandler stateVar connidOrNonce action = normal `catch` exceptional
    where
    normal :: m t
    normal = do
        t <- action
        signalFinished (connidOrNonce, Nothing)
        pure t
    exceptional :: SomeException -> m t
    exceptional e = do
        signalFinished (connidOrNonce, Just e)
        throw e
    -- Signal that a thread handling a given ConnectionId is finished.
    -- It's very important that this is run to completion for every thread
    -- spawned by the dispatcher, else the DispatcherState will never forget
    -- the entry for this ConnectionId.
    signalFinished :: (Either NT.ConnectionId Nonce, Maybe SomeException) -> m ()
    signalFinished outcome = modifySharedAtomic stateVar $ \(NodeState prng nonces finished) ->
            pure ((NodeState prng nonces (outcome : finished)), ())

controlHeaderCodeBidirectionalSyn :: Word8
controlHeaderCodeBidirectionalSyn = fromIntegral (fromEnum 'S')

controlHeaderCodeBidirectionalAck :: Word8
controlHeaderCodeBidirectionalAck = fromIntegral (fromEnum 'A')

controlHeaderCodeUnidirectional :: Word8
controlHeaderCodeUnidirectional = fromIntegral (fromEnum 'U')

controlHeaderUnidirectional :: BS.ByteString
controlHeaderUnidirectional =
    BS.singleton controlHeaderCodeUnidirectional

controlHeaderBidirectionalSyn :: Nonce -> BS.ByteString
controlHeaderBidirectionalSyn (Nonce nonce) =
    fixedSizeBuilder 9 $
        BS.word8 controlHeaderCodeBidirectionalSyn
     <> BS.word64BE nonce

controlHeaderBidirectionalAck :: Nonce -> BS.ByteString
controlHeaderBidirectionalAck (Nonce nonce) =
    fixedSizeBuilder 9 $
        BS.word8 controlHeaderCodeBidirectionalAck
     <> BS.word64BE nonce

fixedSizeBuilder :: Int -> BS.Builder -> BS.ByteString
fixedSizeBuilder n =
    LBS.toStrict . BS.toLazyByteStringWith (BS.untrimmedStrategy n n) LBS.empty

-- | Connect to a peer given by a 'NodeId' bidirectionally.
connectInOutChannel
    :: ( Mockable Channel.Channel m, Mockable Fork m, Mockable SharedAtomic m
       , Mockable Throw m )
    => Node m
    -> NodeId
    -> m (Nonce, ChannelIn m, ChannelOut m)
connectInOutChannel node@Node{nodeEndPoint, nodeState}
                    (NodeId endpointaddr) = do
    mconn <- NT.connect
               nodeEndPoint
               endpointaddr
               NT.ReliableOrdered
               NT.ConnectHints{ connectTimeout = Nothing } --TODO use timeout

    -- TODO: Any error detected here needs to be reported because it's not
    -- reported via the dispatcher thread. It means we cannot establish a
    -- connection in the first place, e.g. timeout.
    case mconn of
      Left  err  -> throw err
      Right outconn -> do
        (nonce, inchan) <- allocateInChannel
        -- TODO: error handling
        NT.send outconn [controlHeaderBidirectionalSyn nonce]
        return (nonce, ChannelIn inchan, ChannelOut outconn)
  where
    allocateInChannel = do
      tid   <- myThreadId
      chan  <- Channel.newChannel
      -- Create a nonce and update the shared atomic so that the nonce indicates
      -- that there's a handler for it.
      nonce <- modifySharedAtomic nodeState $ \(NodeState prng expected finished) -> do
                 let (nonce, !prng') = random prng
                     !expected' = Map.insert nonce (NonceHandlerNotConnected tid (ChannelIn chan)) expected
                 pure ((NodeState prng' expected' finished), nonce)
      return (nonce, chan)

-- | Connect to a peer given by a 'NodeId' unidirectionally.
connectOutChannel
    :: ( Monad m, Mockable Throw m )
    => Node m
    -> NodeId
    -> m (ChannelOut m)
connectOutChannel Node{nodeEndPoint} (NodeId endpointaddr) = do
    mconn <- NT.connect
               nodeEndPoint
               endpointaddr
               NT.ReliableOrdered
               NT.ConnectHints{ connectTimeout = Nothing } --TODO use timeout

    -- TODO: Any error detected here needs to be reported because it's not
    -- reported via the dispatcher thread. It means we cannot establish a
    -- connection in the first place, e.g. timeout.
    case mconn of
      Left  err  -> throw err
      Right conn -> do
        -- TODO error handling
        NT.send conn [controlHeaderUnidirectional]
        return (ChannelOut conn)

closeChannel :: ChannelOut m -> m ()
closeChannel (ChannelOut conn) = NT.close conn

-- | Create, use, and tear down a conversation channel with a given peer
--   (NodeId).
withInOutChannel
    :: forall m a .
       ( Mockable Bracket m, Mockable Fork m, Mockable Channel.Channel m
       , Mockable SharedAtomic m, Mockable Throw m, Mockable Catch m )
    => Node m
    -> NodeId
    -> (ChannelIn m -> ChannelOut m -> m a)
    -> m a
withInOutChannel node@Node{nodeState} nodeid action =
    -- connectInOurChannel will update the nonce state to indicate that there's
    -- a handler for it. When the handler is finished (whether normally or
    -- exceptionally) we have to update it to say so.
    bracket (connectInOutChannel node nodeid)
            (\(_, _, outchan) -> closeChannel outchan)
            (\(nonce, inchan, outchan) -> action' nonce inchan outchan)
    where
    -- Updates the nonce state map always, and re-throws any caught exception.
    action' nonce inchan outchan = finishHandler nodeState (Right nonce) (action inchan outchan)

-- | Create, use, and tear down a unidirectional channel to a peer identified
--   by 'NodeId'.
withOutChannel
    :: ( Mockable Bracket m, Mockable Throw m )
    => Node m
    -> NodeId
    -> (ChannelOut m -> m a)
    -> m a
withOutChannel node nodeid =
    bracket (connectOutChannel node nodeid) closeChannel

-- | Write some ByteStrings to an out channel. It does not close the
--   transport when finished. If you want that, use withOutChannel or
--   withInOutChannel.
writeChannel :: ( Monad m ) => ChannelOut m -> [BS.ByteString] -> m ()
writeChannel (ChannelOut _) [] = pure ()
writeChannel (ChannelOut conn) (chunk:chunks) = do
    res <- NT.send conn [chunk]
    -- Any error detected here will be reported to the dispatcher thread
    -- so we don't need to do anything
     --TODO: though we could log here
    case res of
      Left _err -> return ()
      Right _   -> writeChannel (ChannelOut conn) chunks

-- | Read a 'ChannelIn', blocking until the next 'ByteString' arrives, or end
--   of input is signalled via 'Nothing'.
readChannel :: ( Mockable Channel.Channel m ) => ChannelIn m -> m (Maybe BS.ByteString)
readChannel (ChannelIn chan) = Channel.readChannel chan
