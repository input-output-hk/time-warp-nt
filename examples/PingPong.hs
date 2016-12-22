{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}

import Control.Monad (forM_)
import Control.Monad.IO.Class (liftIO)
import Data.String (fromString)
import Data.Void (Void)
import Data.Binary
import Node
import qualified Network.Transport.TCP as TCP
import Network.Transport.Abstract (newEndPoint)
import Network.Transport.Concrete (concrete)
import System.Random
import Mockable.Concurrent (delay)
import Mockable.Production

-- Sending a message which encodes to "" is problematic!
-- The receiver can't distinuish this from the case in which the sender sent
-- nothing at all.
-- So we give custom Ping and Pong types with non-generic Binary instances.
--
-- TBD should we fix this in network-transport? Maybe every chunk is prefixed
-- by a byte giving its length? Wasteful I guess but maybe not a problem.

data Pong = Pong
deriving instance Show Pong
instance Binary Pong where
    put _ = putWord8 (fromIntegral 1)
    get = do
        w <- getWord8
        if w == fromIntegral 1
        then pure Pong
        else fail "no parse pong"

workers :: NodeId -> StdGen -> [NodeId] -> [Worker Production]
workers id gen peerIds = [pingWorker gen]
    where
    pingWorker :: StdGen -> SendActions Production -> Production ()
    pingWorker gen sendActions = loop gen
        where
        loop :: StdGen -> Production ()
        loop gen = do
            let (i, gen') = randomR (0,1000000) gen
            delay i
            let pong :: NodeId -> ConversationActions Void Pong Production -> Production ()
                pong peerId cactions = do
                    liftIO . putStrLn $ show id ++ " sent PING to " ++ show peerId
                    received <- recv cactions
                    case received of
                        Just Pong -> liftIO . putStrLn $ show id ++ " heard PONG from " ++ show peerId
                        Nothing -> error "Unexpected end of input"
            forM_ peerIds $ \peerId -> withConnectionTo sendActions peerId (fromString "ping") (pong peerId)
            loop gen'

listeners :: NodeId -> [Listener Production]
listeners id = [Listener (fromString "ping") pongWorker]
    where
    pongWorker :: ListenerAction Production
    pongWorker = ListenerActionConversation $ \peerId (cactions :: ConversationActions Pong Void Production) -> do
        liftIO . putStrLn $ show id ++  " heard PING from " ++ show peerId
        send cactions Pong
        liftIO . putStrLn $ show id ++ " sent PONG to " ++ show peerId

main = runProduction $ do

    Right transport1_ <- liftIO $ TCP.createTransport ("0.0.0.0") ("10128") TCP.defaultTCPParameters
    Right transport2_ <- liftIO $ TCP.createTransport ("0.0.0.0") ("10129") TCP.defaultTCPParameters
    let transport1 = concrete transport1_
    let transport2 = concrete transport2_
    Right endpoint1 <- newEndPoint transport1
    Right endpoint2 <- newEndPoint transport2

    let prng1 = mkStdGen 0
    let prng2 = mkStdGen 1
    let prng3 = mkStdGen 2
    let prng4 = mkStdGen 3

    let policy1 = noDelayPolicy
    let policy2 :: DispatchPolicy Production
        policy2 = noDelayPolicy -- uniformDelayPolicy ((1/2) :: Rational)
    -- Randomly delay between 0 and 5 seconds.
    let policy3 :: DispatchPolicy Production
        policy3 = noDelayPolicy -- randomDelayPolicy $ (*) 5

    liftIO . putStrLn $ "Starting nodes"
    rec { node1 <- startNode endpoint1 prng1 policy1 (workers nodeId1 prng2 [nodeId2]) (listeners nodeId1)
        ; node2 <- startNode endpoint2 prng3 policy3 (workers nodeId2 prng4 [nodeId1]) (listeners nodeId2)
        ; let nodeId1 = nodeId node1
        ; let nodeId2 = nodeId node2
        }

    liftIO . putStrLn $ "Hit return to stop"
    _ <- liftIO getChar

    liftIO . putStrLn $ "Stopping node"
    stopNode node1
    stopNode node2
