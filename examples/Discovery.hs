{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Monad (forM_, forM, when, mzero)
import Control.Monad.IO.Class (liftIO)
import Data.String (fromString)
import Data.Binary
import Data.Void (Void)
import qualified Data.Set as S
import qualified Data.ByteString.Char8 as B8
import Node
import qualified Network.Transport.TCP as TCP
import Network.Transport.Concrete (concrete)
import Network.Transport.Abstract (newEndPoint)
import Network.Discovery.Abstract
import qualified Network.Discovery.Transport.Kademlia as K
import System.Environment (getArgs)
import System.Random
import Mockable.Concurrent (fork, delay, runInUnboundThread)
import Mockable.Production

data Pong = Pong
deriving instance Show Pong
instance Binary Pong where
    put _ = putWord8 (fromIntegral 1)
    get = do
        w <- getWord8
        if w == fromIntegral 1
        then pure Pong
        else fail "no parse pong"

workers :: NodeId -> StdGen -> NetworkDiscovery K.KademliaDiscoveryErrorCode Production -> [Worker Production]
workers id gen discovery = [pingWorker gen]
    where
    pingWorker :: StdGen -> SendActions Production -> Production ()
    pingWorker gen sendActions = loop gen
        where
        loop gen = do
            let (i, gen') = randomR (0,2000000) gen
            delay i
            peerSet_ <- knownPeers discovery
            discoverPeers discovery
            peerSet <- knownPeers discovery
            liftIO . putStrLn $ show id ++ " has peer set: " ++ show peerSet
            -- For each peer, fork a thread to do a PING/PONG conversation
            -- with it, and *do not* wait for it.
            forM_ (S.toList peerSet) $ \addr -> fork $ withConnectionTo sendActions (NodeId addr) (fromString "ping") $
                \(cactions :: ConversationActions Void Pong Production) -> do
                    received <- recv cactions
                    case received of
                        Just Pong -> liftIO . putStrLn $ show id ++ " heard PONG from " ++ show addr
                        Nothing -> error "Unexpected end of input"
            loop gen'

listeners :: NodeId -> [Listener Production]
listeners id = [Listener (fromString "ping") pongListener]
    where
    pongListener :: ListenerAction Production
    pongListener = ListenerActionConversation $ \peerId (cactions :: ConversationActions Pong Void Production) -> do
        liftIO . putStrLn $ show id ++  " heard PING from " ++ show peerId
        send cactions Pong

makeNode i = do
    let tcpport = 10128 + i
    let udpport = 3000 + i
    let host = "127.0.0.1"
    let tcpParameters = TCP.defaultTCPParameters {TCP.tcpUserTimeout = Just 10}
    Right transport_ <- liftIO $ TCP.createTransport ("127.0.0.1") (show tcpport) tcpParameters
    let transport = concrete transport_
    let id = makeId i
    let initialPeer =
            if i == 0
            -- First node uses itself as initial peer, else it'll panic because
            -- its initial peer appears to be down.
            then K.Node (K.Peer host (fromIntegral udpport)) id
            else K.Node (K.Peer host (fromIntegral (udpport - 1))) (makeId (i - 1))
    let kademliaConfig = K.KademliaConfiguration (fromIntegral udpport) id
    let prng1 = mkStdGen (2 * i)
    let prng2 = mkStdGen ((2 * i) + 1)
    -- 5 second delay on all events for the 0'th node, no delay for the others.
    let policy :: DispatchPolicy Production
        policy = if i == -1 then uniformDelayPolicy (5 :: Int) else noDelayPolicy
        {-
        policy = do x <- randomDelayPolicy steppedDelay
                    if (x == 0) then mzero else pure x
        -}
    liftIO . putStrLn $ "Starting node " ++ show i
    Right endPoint <- newEndPoint transport
    rec { node <- startNode endPoint prng1 policy (workers (nodeId node) prng2 discovery) (listeners (nodeId node))
        ; let localAddress = nodeEndPointAddress node
        ; liftIO . putStrLn $ "Making discovery for node " ++ show i
        ; discovery <- K.kademliaDiscovery kademliaConfig initialPeer localAddress
        }
    pure (node, discovery)
    where
    makeId i
        | i < 10 = B8.pack ("node_identifier_0" ++ show i)
        | otherwise = B8.pack ("node_identifier_" ++ show i)

    steppedDelay d
        | d > 0.99 = 10
        | d > 0.9 = 5
        | otherwise = 0

main = runProduction $ do

    [arg0] <- liftIO getArgs
    let number = read arg0

    when (number > 99 || number < 1) $ error "Give a number in [1,99]"

    runInUnboundThread $ do

        liftIO . putStrLn $ "Spawning " ++ show number ++ " nodes"
        nodesAndDiscoveries <- forM [0..number] makeNode

        liftIO $ putStrLn "Hit return to stop"
        _ <- liftIO $ getChar

        liftIO $ putStrLn "Stopping nodes"
        forM_ nodesAndDiscoveries (\(n, d) -> stopNode n >> closeDiscovery d)

-- TODO add a feature: some options so that we can spin this up in multiple
-- processes
