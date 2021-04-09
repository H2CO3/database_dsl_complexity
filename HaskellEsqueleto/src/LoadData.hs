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

module LoadData where

import GHC.Int (Int64)
import Data.Maybe (fromJust)
import Data.Text (Text, pack, unpack)
import Text.Casing (pascal)
import Data.Time.Clock (UTCTime)
import Database.SQLite3
import Database.Persist (Entity(Entity))
import Database.Persist.Sql (toSqlKey)
import Schema

-- RAII-style connection and prepared statement management

withDatabase :: Text -> (Database -> IO a) -> IO a
withDatabase fileName function = do
  db <- open fileName
  result <- function db
  close db
  return result

withStatement :: Database -> Text -> [SQLData] -> IO [[SQLData]]
withStatement db sql params = do
  stmt <- prepare db sql
  mapM_ (\(index, param) -> bindSQLData stmt index param) (zip [1..] params)
  rows <- allRows stmt
  finalize stmt
  return rows

-- Generic "get stuff into memory" functions

allRows_ :: Statement -> [[SQLData]] -> IO [[SQLData]]
allRows_ stmt rows = do
  result <- step stmt
  case result of
    Done -> return rows
    Row -> do
      row <- columns stmt
      moreRows <- allRows_ stmt rows
      return $ row : moreRows

allRows :: Statement -> IO [[SQLData]]
allRows stmt = allRows_ stmt []

-- Functions for constructing strongly-typed domain model values
-- from loosely-typed SQL values.

parseDate :: Text -> UTCTime
parseDate = read . (++ "Z") . unpack

geoCoordFromRow :: [SQLData] -> Entity GeoCoord
geoCoordFromRow [SQLInteger id, SQLInteger regionId, SQLFloat lat, SQLFloat lon]
  = Entity (toSqlKey id) (GeoCoord (toSqlKey regionId) lat lon)
geoCoordFromRow row@_ = error $ "Invalid GeoCoord row: " ++ (show row)

allGeoCoords :: Database -> IO [Entity GeoCoord]
allGeoCoords db = do
  let sql = "SELECT id, region_id, latitude, longitude FROM location ORDER BY region_id, id"
  withStatement db sql [] >>= return . map geoCoordFromRow

regionFromRow :: [SQLData] -> Entity Region
regionFromRow [SQLInteger id, SQLText name, SQLNull]
  = regionFromRow_ id name Nothing
regionFromRow [SQLInteger id, SQLText name, SQLInteger parentId]
  = regionFromRow_ id name (Just parentId)
regionFromRow row@_ = error $ "Invalid Region row: " ++ (show row)

regionFromRow_ :: Int64 -> Text -> Maybe Int64 -> Entity Region
regionFromRow_ id name parentId =
  Entity (toSqlKey id) (Region name $ fmap toSqlKey parentId)

allRegions :: Database -> IO [Entity Region]
allRegions db = do
  -- Use recursive CTE to query the hierarchy in the correct order,
  -- starting with the parents and descending down level-by-level.
  -- This is needed for upholding the foreign key constraints when
  -- re-inserting the data into the schema of Persistent and Esqueleto.
  let sql = "WITH tmp AS ( \
            \    SELECT id, name, parent_id, CAST(id AS TEXT) AS path \
            \    FROM region \
            \    WHERE parent_id IS NULL \
            \  UNION ALL \
            \    SELECT region.id, region.name, region.parent_id, (tmp.path || '/' || CAST(region.id AS TEXT)) AS path \
            \    FROM region \
            \    INNER JOIN tmp \
            \    ON region.parent_id = tmp.id \
            \) \
            \  SELECT id, name, parent_id \
            \  FROM tmp \
            \  ORDER BY path"

  withStatement db sql [] >>= return . map regionFromRow

allRealEstates :: Database -> IO [Entity RealEstate]
allRealEstates db = do
  let sql = "SELECT id, kind, owner_id, region_id FROM real_estate ORDER BY id"
  withStatement db sql [] >>= return . map realEstateFromRow

realEstateFromRow :: [SQLData] -> Entity RealEstate
realEstateFromRow [SQLInteger id, SQLText kindText, SQLInteger ownerId, SQLInteger regionId]
  = Entity
      (toSqlKey id)
      (RealEstate
         kind
         (toSqlKey ownerId)
         (toSqlKey regionId))
  where
    kind = read $ pascal $ unpack kindText
realEstateFromRow row@_ = error $ "Invalid RealEstate row: " ++ (show row)

allBookings :: Database -> IO [Entity Booking]
allBookings db = do
  let sql = "SELECT id, real_estate_id, user_id, start_date, end_date, price FROM booking ORDER BY id"
  withStatement db sql [] >>= return . map bookingFromRow

bookingFromRow :: [SQLData] -> Entity Booking
bookingFromRow [ SQLInteger id
               , SQLInteger realEstateId
               , SQLInteger userId
               , SQLText startDate
               , SQLText endDate
               , SQLFloat price
               ] = Entity
                     (toSqlKey id)
                     (Booking
                       (toSqlKey realEstateId)
                       (toSqlKey userId)
                       (parseDate startDate)
                       (parseDate endDate)
                       price)
bookingFromRow [ SQLInteger id
               , SQLInteger realEstateId
               , SQLInteger userId
               , SQLText startDate
               , SQLText endDate
               , SQLInteger price
               ] = Entity
                     (toSqlKey id)
                     (Booking
                       (toSqlKey realEstateId)
                       (toSqlKey userId)
                       (parseDate startDate)
                       (parseDate endDate)
                       (fromIntegral price))
bookingFromRow row@_ = error $ "Invalid Booking row: " ++ (show row)

allSessions :: Database -> IO [Entity Session]
allSessions db = do
  let sql = "SELECT id, user_id, login_public_key, login_date, logout_public_key, logout_date FROM session ORDER BY user_id, id"
  withStatement db sql [] >>= return . map sessionFromRow

sessionFromRow :: [SQLData] -> Entity Session
sessionFromRow [ SQLInteger id
               , SQLInteger userId
               , SQLBlob loginPubkey
               , SQLText loginDate
               , SQLBlob logoutPubkey
               , SQLText logoutDate
               ] = Entity
                     (toSqlKey id)
                     (Session
                       (toSqlKey userId)
                       (AuthEvent loginPubkey $ parseDate loginDate)
                       (Just $ AuthEvent logoutPubkey $ parseDate loginDate))
sessionFromRow [ SQLInteger id
               , SQLInteger userId
               , SQLBlob loginPubkey
               , SQLText loginDate
               , SQLNull
               , SQLNull
               ] = Entity
                     (toSqlKey id)
                     (Session
                       (toSqlKey userId)
                       (AuthEvent loginPubkey $ parseDate loginDate)
                       Nothing)
sessionFromRow row@_ = error $ "Invalid Session row: " ++ (show row)

allUserProfiles :: Database -> IO [UserProfile]
allUserProfiles db = do
  let sql = "SELECT id, user_id FROM profile ORDER BY user_id"
  rows <- withStatement db sql []
  return $ (flip map) rows $ \[SQLInteger profileId, SQLInteger userId] ->
    UserProfile (toSqlKey userId) (toSqlKey profileId)

allProfiles :: Database -> IO [Entity Profile]
allProfiles db = do
  let sql = "SELECT id, facebook_id, google_id, internal_id FROM profile ORDER BY user_id"
  withStatement db sql [] >>= return . map profileFromRow

profileFromRow :: [SQLData] -> Entity Profile
profileFromRow [ SQLInteger profileId
               , SQLInteger facebookId
               , SQLNull
               , SQLNull
               ] =
  Entity (toSqlKey profileId) (ProfileFacebookSum $ toSqlKey facebookId)
profileFromRow [ SQLInteger profileId
               , SQLNull
               , SQLInteger googleId
               , SQLNull
               ] =
  Entity (toSqlKey profileId) (ProfileGoogleSum $ toSqlKey googleId)
profileFromRow [ SQLInteger profileId
               , SQLNull
               , SQLNull
               , SQLInteger internalId
               ] =
  Entity (toSqlKey profileId) (ProfileInternalSum $ toSqlKey internalId)
profileFromRow row@_ = error $ "Invalid Profile row: " ++ (show row)

allFacebookProfiles :: Database -> IO [Entity FacebookProfile]
allFacebookProfiles db = do
  let sql = "SELECT id, facebook_account_id FROM profile_facebook ORDER BY id"
  withStatement db sql [] >>= return . map facebookProfileFromRow

facebookProfileFromRow :: [SQLData] -> Entity FacebookProfile
facebookProfileFromRow [SQLInteger id, SQLText accountId] =
  Entity (toSqlKey id) (FacebookProfile accountId)
facebookProfileFromRow row@_ = error $ "Invalid FacebookProfile row: " ++ (show row)

allGoogleProfiles :: Database -> IO [Entity GoogleProfile]
allGoogleProfiles db = do
  let sql = "SELECT id, google_account_id, email, image_url FROM profile_google ORDER BY id"
  withStatement db sql [] >>= return . map googleProfileFromRow

googleProfileFromRow :: [SQLData] -> Entity GoogleProfile
googleProfileFromRow [SQLInteger id, SQLText accountId, SQLText email, SQLText imageUrl] =
  Entity (toSqlKey id) (GoogleProfile accountId (Just email) (Just imageUrl))
googleProfileFromRow [SQLInteger id, SQLText accountId, SQLText email, SQLNull] =
  Entity (toSqlKey id) (GoogleProfile accountId (Just email) Nothing)
googleProfileFromRow [SQLInteger id, SQLText accountId, SQLNull, SQLText imageUrl] =
  Entity (toSqlKey id) (GoogleProfile accountId Nothing (Just imageUrl))
googleProfileFromRow [SQLInteger id, SQLText accountId, SQLNull, SQLNull] =
  Entity (toSqlKey id) (GoogleProfile accountId Nothing Nothing)
googleProfileFromRow row@_ = error $ "Invalid GoogleProfile row: " ++ (show row)

allInternalProfiles :: Database -> IO [Entity InternalProfile]
allInternalProfiles db = do
  let sql = "SELECT id, password_hash, password_salt FROM profile_internal ORDER BY id"
  withStatement db sql [] >>= return . map internalProfileFromRow

internalProfileFromRow :: [SQLData] -> Entity InternalProfile
internalProfileFromRow [SQLInteger id, SQLBlob pwHash, SQLBlob pwSalt] =
  Entity (toSqlKey id) (InternalProfile pwHash pwSalt)
internalProfileFromRow row@_ = error $ "Invalid InternalProfile row: " ++ (show row)

allUsers :: Database -> IO [Entity User]
allUsers db = do
  let sql = "SELECT id, username, real_name, birth_date FROM user ORDER BY id"
  withStatement db sql [] >>= return . map userFromRow

userFromRow :: [SQLData] -> Entity User
userFromRow [SQLInteger id, SQLText username, SQLText realName, SQLText birthDate]
  = userFromRow_ id username (Just realName) (Just birthDate)
userFromRow [SQLInteger id, SQLText username, SQLText realName, SQLNull]
  = userFromRow_ id username (Just realName) Nothing
userFromRow [SQLInteger id, SQLText username, SQLNull, SQLText birthDate]
  = userFromRow_ id username Nothing (Just birthDate)
userFromRow [SQLInteger id, SQLText username, SQLNull, SQLNull]
  = userFromRow_ id username Nothing Nothing
userFromRow row@_ = error $ "Invalid User row: " ++ (show row)

userFromRow_ :: Int64 -> Text -> Maybe Text -> Maybe Text -> Entity User
userFromRow_ id username realName birthDate =
  Entity (toSqlKey id) (User username realName (fmap parseDate birthDate))

allCategories :: [Entity Category]
allCategories = [ Entity (toSqlKey 1) $ Category "days" 0
                , Entity (toSqlKey 2) $ Category "weeks" 7
                , Entity (toSqlKey 3) $ Category "months" 30
                , Entity (toSqlKey 4) $ Category "years" 365
                , Entity (toSqlKey 5) $ Category "centuries" 36525
                ]
