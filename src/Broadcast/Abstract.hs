module Broadcast.Abstract (

      Broadcast

    , Initiator
    , Repeater
    , WithRepeater

    ) where

import Node

-- | A broadcast constructs what's necessary to initiate and repeat messages
--   for a given name.
type Broadcast packing body m = MessageName -> m (Initiator packing body m, WithRepeater packing body m)

-- | Initiate a broadcast of a given payload.
type Initiator packing body m = body -> SendActions packing m -> m ()

-- | Repeat the broadcast of a given payload.
type Repeater m = m ()

type WithRepeater packing body m =
     (Repeater m -> NodeId -> SendActions packing m -> body -> m ())
  -> Listener packing m
