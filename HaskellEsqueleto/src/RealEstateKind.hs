{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE FlexibleContexts           #-}

-- This module only exists because TemplateHaskell expansions
-- need to be in a different module than the one they are used in.
module RealEstateKind where

import GHC.Generics
import Database.Persist.TH
import Data.Aeson.Types
import Data.Aeson (ToJSON, FromJSON)

data RealEstateKind = Apartment | House | Mansion | Penthouse
  deriving (Eq, Show, Read, Generic, ToJSON, FromJSON)

derivePersistField "RealEstateKind"
