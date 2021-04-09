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
{-# LANGUAGE PartialTypeSignatures      #-}

module Schema where

import GHC.Generics
import Text.Hex (encodeHex, decodeHex)
import Data.Maybe (fromJust)
import Data.Text hiding (map)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.ByteString hiding (map)
import Data.Time.Clock (UTCTime)
import Data.Aeson (ToJSON, toJSON, FromJSON, parseJSON)
import Database.Esqueleto
import Database.Persist hiding ((==.))
import Database.Persist.TH

import RealEstateKind

share [mkPersist sqlSettings, mkMigrate "migrateAll"]
  [persistLowerCase|
  User
    username   Text
    realName   Text Maybe
    birthDate  UTCTime Maybe
    UniqueName username
    deriving   Eq Show Generic ToJSON FromJSON

  FacebookProfile
    accountId  Text
    deriving   Eq Show Generic ToJSON FromJSON

  GoogleProfile
    accountId  Text
    email      Text Maybe
    imageUrl   Text Maybe
    deriving   Eq Show Generic ToJSON FromJSON

  InternalProfile
    pwHash     ByteString
    pwSalt     ByteString
    deriving   Eq Show Generic ToJSON FromJSON

  -- This junction table is only needed because it is not possible to put other fields
  -- directly inside sum types, and we want Profile to have a UserId for realizing the
  -- User 1 <-> N Profile connection.
  -- We already tried several different approaches before arriving at this solution:
  --   * Renaming the current `Profile` type to `ProfileSum`, and creating a record
  --     type `Profile` with fields `userId :: UserId` and `inner :: ProfileSum`.
  --     This does not work because then one entity directly contains another.
  --   * We tried putting a list of profiles directly into the `User` entity.
  --     This is accepted by Persistent, but it ends up serializing the profiles
  --     as JSON and dumping them into a text column, with no support for projecting
  --     over the inner fields when trying to use them from an Esqueleto query.
  --   * Also, seriously, dear framework, dumping JSON in a text field?
  UserProfile
    user       UserId
    profile    ProfileId
    UniqueId   profile
    deriving   Eq Show Generic ToJSON FromJSON

  +Profile
    facebook   FacebookProfileId
    google     GoogleProfileId
    internal   InternalProfileId
    deriving   Eq Show Generic ToJSON FromJSON

  Session
    user       UserId
    login      AuthEvent
    logout     AuthEvent Maybe
    deriving   Eq Show Generic ToJSON FromJSON

  AuthEvent
    pubKey     ByteString
    date       UTCTime
    deriving   Eq Show Generic ToJSON FromJSON

  RealEstate
    kind       RealEstateKind
    owner      UserId
    region     RegionId
    deriving   Eq Show Generic ToJSON FromJSON

  Region
    name       Text
    parent     RegionId Maybe
    deriving   Eq Show Generic ToJSON FromJSON

  GeoCoord
    region     RegionId
    latitude   Double
    longitude  Double
    deriving   Eq Show Generic ToJSON FromJSON

  Booking
    realEstate RealEstateId
    user       UserId
    startDate  UTCTime
    endDate    UTCTime
    price      Double
    deriving   Eq Show Generic ToJSON FromJSON

  -- This helper table only exists because Esqueleto
  -- does not support inline tables (the `VALUES (...)` clause).
  -- This is used in the `queryAvgDailyPriceByUserByBookingLengthCategory`
  -- function.
  Category
    name       Text
    minDays    Double
  |]

instance ToJSON ByteString where
  toJSON = toJSON . encodeHex

instance FromJSON ByteString where
  parseJSON v = parseJSON v >>= return . fromJust . decodeHex

instance ToJSON a => ToJSON (Value a) where
  toJSON (Value v) = toJSON v
