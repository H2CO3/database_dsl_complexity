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
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE PartialTypeSignatures      #-}

module Queries where

import GHC.Int (Int64)
import GHC.Generics hiding (from)
import Prelude hiding (id)
import Data.Text hiding (map, takeWhile, groupBy, count)
import Data.ByteString hiding (map, takeWhile, groupBy, count)
import Data.Time.Clock (UTCTime)
import Data.Aeson (ToJSON, FromJSON)
import qualified Data.Aeson as J
import Data.Aeson.Casing (snakeCase)
import Data.Aeson.TH (deriveJSON, defaultOptions, Options(fieldLabelModifier))
import Database.Esqueleto
import Database.Esqueleto.Internal.Internal (unsafeSqlFunction)
import qualified Database.Esqueleto.Experimental as E
import Database.Persist hiding (count, (==.), (!=.), (<.), (<=.), (>.), (>=.), (||.), (&&.))
import Database.Persist.TH

import Schema
import ToJSONRow (toJSONRow)


--
-- General helpers
--
valueToInt :: J.Value -> Int64
valueToInt (J.Number x) = read $ takeWhile (/= '.') $ show x -- sorry for this but it's simple


--
-- Get all continents, i.e. all regions with no parent.
--

toRegionResult :: (Value RegionId, Value Text, Value (Maybe RegionId)) -> [(Text, J.Value)]
toRegionResult = toJSONRow ("id" :: Text, "name" :: Text, "parent_id" :: Text)

queryContinents :: [J.Value] -> _
queryContinents [] = from $ \r -> do
  where_ $ isNothing $ r ^. RegionParent
  orderBy [asc $ r ^. RegionId]
  return (r ^. RegionId, r ^. RegionName, r ^. RegionParent)

--
-- Get siblings and parents of a given region,
-- and do the same recursively at each level.
--

querySiblingsAndParents :: [J.Value] -> _
querySiblingsAndParents [regionIdVal] = distinct $ do
  let regionId = toSqlKey $ valueToInt regionIdVal

  cte <- E.withRecursive
    (from $ \r -> do
        where_ $ r ^. RegionId ==. val regionId
        return (r ^. RegionId, r ^. RegionName, r ^. RegionParent))
    E.unionAll_
    (\self -> do
        (_, _, childParent) <- E.from self
        from $ \parent -> do
          where_ $ just (parent ^. RegionId) ==. childParent
          return (parent ^. RegionId, parent ^. RegionName, parent ^. RegionParent))

  (childId, childName, childParent) <- E.from cte
  sibling <- E.from $ E.Table @Region

  where_ $
    childParent ==. sibling ^. RegionParent
    ||.
    isNothing (sibling ^. RegionParent)

  orderBy [ desc $ sibling ^. RegionParent
          , asc $ sibling ^. RegionId
          ]

  return (sibling ^. RegionId, sibling ^. RegionName, sibling ^. RegionParent)

--
-- Get users with no sessions ever initiated.
--

toUserIdResult :: (Value UserId) -> [(Text, J.Value)]
toUserIdResult = toJSONRow ("user_id" :: Text)

queryNoLoginUsers :: [J.Value] -> _
queryNoLoginUsers [] = from $ \(user `LeftOuterJoin` session) -> do
  on $ just (user ^. UserId) ==. session ?. SessionUser
  where_ $ isNothing $ session ?. SessionId
  orderBy [asc $ user ^. UserId]
  return $ user ^. UserId

---
--- Get number of currently valid sessions for each user.
---

toSessionCountResult :: (Value UserId, Value Int64) -> [(Text, J.Value)]
toSessionCountResult = toJSONRow ("user_id" :: Text, "valid_sessions" :: Text)

queryNumValidSessions :: [J.Value] -> SqlQuery (SqlExpr (Value (Key User)), SqlExpr (Value Int64))
queryNumValidSessions [] = from $ \(user `LeftOuterJoin` session) -> do
  on $ just (user ^. UserId) ==. session ?. SessionUser
  groupBy $ user ^. UserId
  orderBy [asc $ user ^. UserId]
  return ( user ^. UserId
         , coalesceDefault [
             sum_ $ case_ [
               when_
                 ((isNothing $ session ?. SessionLogout)
                 &&.
                 (not_ $ isNothing $ session ?. SessionId))
               then_ $ val (1 :: Int64)
             ]
             (else_ $ val 0)
           ] $ val 0
         )

--
-- Get users having strictly more than one profile.
--

toProfileCountResult :: (Value UserId, Value Int64) -> [(Text, J.Value)]
toProfileCountResult = toJSONRow ("user_id" :: Text, "profile_count" :: Text)

queryMultiProfileUsers :: [J.Value] -> _
queryMultiProfileUsers [] = from $ \userProfile -> do
  let userId = userProfile ^. UserProfileUser
  let profileId = userProfile ^. UserProfileProfile
  groupBy userId
  let profileCount :: _ (_ Int64) = count profileId
  having $ profileCount >. val 1
  orderBy [asc userId]
  return (userId, profileCount)

--
-- Number of (unique) regions each user owns real estate in.
--

toRegionCountResult :: (Value UserId, Value Int64) -> [(Text, J.Value)]
toRegionCountResult = toJSONRow ("user_id" :: Text, "region_count" :: Text)

queryOwnedRealEstateRegionCount :: [J.Value] -> _ (_, _ (Value Int64))
queryOwnedRealEstateRegionCount [] = from $ \(user `LeftOuterJoin` realEstate) -> do
  on $ realEstate ^. RealEstateOwner ==. user ^. UserId
  groupBy $ user ^. UserId
  orderBy [asc $ user ^. UserId]
  return ( user ^. UserId
         , castNum $ countDistinct $ realEstate ^. RealEstateRegion
         )

--
-- Return the number of Facebook and internal profiles
-- for users who lack any Google profiles.
--

toNonGoogleResult :: (Value UserId, Value Int64, Value Int64) -> [(Text, J.Value)]
toNonGoogleResult = toJSONRow ( "user_id" :: Text
                              , "fb_profile_count" :: Text
                              , "internal_profile_count" :: Text
                              )

queryProfileCountsByNonGoogleUser :: [J.Value] -> _ (_, _ (Value Int64), _ (Value Int64))
queryProfileCountsByNonGoogleUser [] = from $ \(user `LeftOuterJoin` userProfile `LeftOuterJoin` profile) -> do
  on $
    user ^. UserId ==. userProfile ^. UserProfileUser
    &&.
    just (userProfile ^. UserProfileProfile) ==. profile ?. ProfileId

  groupBy $ user ^. UserId
  having $ (count $ profile ?. ProfileGoogle) ==. val (0 :: Int64)
  orderBy [asc $ user ^. UserId]

  return ( user ^. UserId
         , castNum $ count $ profile ?. ProfileFacebook
         , castNum $ count $ profile ?. ProfileInternal
         )

--
-- Categorize each booking by discretizing its duration (e.g.:
-- less than a week, weeks, months, etc.).
-- Then, for each user and for each such duration category,
-- report the average price of the bookings, i.e. the total
-- amount paid for the bookings divided by the total duration.
--

toAvgPriceResult :: (Value Text, Value Text, Value (Maybe Double)) -> [(Text, J.Value)]
toAvgPriceResult = toJSONRow ( "username" :: Text
                             , "duration" :: Text
                             , "avg_daily_price" :: Text
                             )

queryAvgDailyPriceByUserByBookingLengthCategory :: [J.Value] -> _
queryAvgDailyPriceByUserByBookingLengthCategory [] = do
  let julianDay :: SqlExpr (Value UTCTime) -> SqlExpr (Value Double)
        = unsafeSqlFunction "JULIANDAY"

  -- Existing user and category entries
  from $ \(user `CrossJoin` category `LeftOuterJoin` booking) -> do
    let days = julianDay (booking ^. BookingEndDate) -. julianDay (booking ^. BookingStartDate)
    let catName = case_ [ when_ (days >=. val 36525) then_ (val "centuries")
                        , when_ (days >=.   val 365) then_ (val "years")
                        , when_ (days >=.    val 30) then_ (val "months")
                        , when_ (days >=.     val 7) then_ (val "weeks")
                        , when_ (days >=.     val 0) then_ (val "days")
                        ]
                  (else_ val "") -- should only happen when there's no such booking anyway
    on $
      booking ^. BookingUser ==. user ^. UserId
      &&.
      category ^. CategoryName ==. catName

    groupBy (user ^. UserId, category ^. CategoryName)

    let avgDailyPrice = ((sum_ $ booking ^. BookingPrice) :: _ (_ (Maybe Double)))
                        /.
                        ((sum_ days) :: _ (_ (Maybe Double)))

    orderBy [asc $ user ^. UserUsername, asc $ category ^. CategoryName]

    return ( user ^. UserUsername
           , category ^. CategoryName
           , avgDailyPrice
           )

--
-- For the specified user, show the top booked regions, i.e.
-- regions in which s/he has the most bookings.
--

toBookedRegionResult :: (Value Text, Value Int64) -> [(Text, J.Value)]
toBookedRegionResult = toJSONRow ("region_name" :: Text, "frequency" :: Text)

queryTopNBookedRegionsForUserX :: [J.Value] -> _
queryTopNBookedRegionsForUserX [userId, topN] = do
  region <- from $ \(booking `InnerJoin` realEstate `InnerJoin` region) -> do
    on $ booking ^. BookingRealEstate ==. realEstate ^. RealEstateId
    on $ region ^. RegionId ==. realEstate ^. RealEstateRegion
    where_ $ booking ^. BookingUser ==. val (toSqlKey $ valueToInt userId)
    limit $ valueToInt topN
    return $ region ^. RegionName

  groupBy region

  let frequency :: _ (_ Int64) = count region
  orderBy [desc frequency, asc region]

  return (region, frequency)

--
-- For each user, find the latitude of the northest point
-- over all regions in which that user has had any bookings.
--
-- This is a more naive, slow implementation, for comparison.
--

toLatitudeResult :: (Value UserId, Value (Maybe Double)) -> [(Text, J.Value)]
toLatitudeResult = toJSONRow ("user_id" :: Text, "northest_latitude" :: Text)

queryNorthestBookedLatitudeSlow :: [J.Value] -> _
queryNorthestBookedLatitudeSlow [] = do
  (user E.:& booking E.:& realEstate E.:& coord) <- E.from $
    E.Table @User
    `LeftOuterJoin` E.Table @Booking
    `E.on` (\(u E.:& b) -> just (u ^. UserId) ==. b ?. BookingUser)
    `LeftOuterJoin` E.Table @RealEstate
    `E.on` (\(u E.:& b E.:& r) -> b ?. BookingRealEstate ==. r ?. RealEstateId)
    `LeftOuterJoin` E.Table @GeoCoord
    `E.on` (\(u E.:& b E.:& r E.:& c) -> r ?. RealEstateRegion ==. c ?. GeoCoordRegion)

  let uid = user ^. UserId
  let maxLat = joinV $ max_ $ coord ?. GeoCoordLatitude

  groupBy uid
  orderBy [asc uid]
  return (uid, maxLat)

--
-- For each user, find the latitude of the northest point
-- over all regions in which that user has had any bookings.
--
-- This is a more optimized, less naive implementation.
--

queryNorthestBookedLatitudeFast :: [J.Value] -> _
queryNorthestBookedLatitudeFast [] = do
  let regionMaxLat = from $ \(region `InnerJoin` coord) -> do
        on $ coord ^. GeoCoordRegion ==. region ^. RegionId
        groupBy $ region ^. RegionId
        return (region ^. RegionId, max_ $ coord ^. GeoCoordLatitude)

  (user E.:& booking E.:& realEstate E.:& (rid, lat)) <- E.from $
    E.Table @User
    `LeftOuterJoin` E.Table @Booking
    `E.on` (\(u E.:& b) -> just (u ^. UserId) ==. b ?. BookingUser)
    `LeftOuterJoin` E.Table @RealEstate
    `E.on` (\(u E.:& b E.:& r) -> b ?. BookingRealEstate ==. r ?. RealEstateId)
    `LeftOuterJoin` regionMaxLat
    `E.on` (\(u E.:& b E.:& r E.:& (rid, lat)) -> r ?. RealEstateRegion ==. rid)

  let uid = user ^. UserId
  let maxLat = joinV $ max_ lat

  groupBy uid
  orderBy [asc uid]
  return (uid, maxLat)
