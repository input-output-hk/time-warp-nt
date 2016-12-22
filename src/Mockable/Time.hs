{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}

module Mockable.Time (

      HasTime(..)
    , GetCurrentTime(..)
    , getCurrentTime

    ) where

import Mockable.Class

class
    ( RealFrac (TimeDelta m)
    ) => HasTime (m :: * -> *)
    where
    type TimeAbsolute m :: *
    -- | A RealFrac in which the whole number part gives seconds.
    type TimeDelta m :: *
    addTime :: TimeAbsolute m -> TimeDelta m -> TimeAbsolute m
    diffTime :: TimeAbsolute m -> TimeAbsolute m -> TimeDelta m

data GetCurrentTime (m :: * -> *) (t :: *) where
    GetCurrentTime :: GetCurrentTime m (TimeAbsolute m)

getCurrentTime :: ( Mockable GetCurrentTime m ) => m (TimeAbsolute m)
getCurrentTime = liftMockable GetCurrentTime
