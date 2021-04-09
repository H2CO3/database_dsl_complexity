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

module Util where

import GHC.Generics
import Prelude hiding (readFile)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Reader (ReaderT)
import Text.Printf (printf)
import Data.Maybe (fromJust)
import Data.HashMap.Strict (fromList)
import Data.Text (Text, pack, unpack)
import Data.Map (Map)
import Data.Text.Lazy (toStrict)
import Data.Text.Lazy.Builder (toLazyText)
import Data.ByteString.Lazy (readFile)
import qualified Data.Aeson as J
import Data.Aeson (ToJSON, toJSON, FromJSON, decode, Value(Array, Object))
import Data.Aeson.Text (encodeToTextBuilder)
import Filesystem (isFile)
import Filesystem.Path.CurrentOS (fromText)
import Database.Persist hiding ((==.))
import Database.Persist.Sqlite hiding ((==.))

import Schema
import Index
import LoadData
import ToJSONRow ((~==.))


data TestCase = TestCase
  { params :: [Value]
  , results :: [Value]
  } deriving (Eq, Show, Generic, ToJSON, FromJSON)

toJSONString :: ToJSON a => a -> String
toJSONString = unpack . toStrict . toLazyText . encodeToTextBuilder . toJSON

toJSONValues :: ToJSON a => (b -> a) -> [b] -> [Value]
toJSONValues f xs = map (toJSON . f) xs

toJSONRows :: [[(Text, J.Value)]] -> [J.Value]
toJSONRows = map (Object . fromList)

readTestCases :: FilePath -> IO (Map String TestCase)
readTestCases path = readFile path >>= return . fromJust . decode

reportResults :: String -> [Value] -> [Value] -> Double -> IO Bool
reportResults name actual expected elapsed = do
  let equals = actual ~==. expected

  if equals then
    printf "%s Succeeded in %.4f s\n----------------\n" name elapsed
  else
    -- TODO(H2CO3): print mismatch indexes
    -- TODO(H2CO3): pretty-print actual and expected results in JSON
    -- TODO(H2CO3): print generated SQL statement
    printf "%s FAILED\n\nActual results: %s\n\nExpected results: %s\n----------------\n"
           name
           (show actual)
           (show expected)

  return equals

insertAll ::
  ( MonadIO m
  , PersistStoreWrite backend
  , PersistEntity entity
  , PersistEntityBackend entity ~ BaseBackend backend
  )
  => [Entity entity] -> ReaderT backend m ()
insertAll entities
  = mapM_
      (\(Entity id entity) -> repsert id entity)
      entities

populateDatabase :: FilePath -> FilePath -> IO ()
populateDatabase sourceDbPath targetDbPath = do
  exists <- isFile $ fromText $ pack targetDbPath

  if exists
    then
      printf "Warning: Database at %s already exists; not reloading data\n" targetDbPath
    else
      withDatabase (pack sourceDbPath) $ \db -> do
        putStrLn "Loading Data"

        users <- allUsers db
        regions <- allRegions db
        geoCoords <- allGeoCoords db
        realEstates <- allRealEstates db
        bookings <- allBookings db
        sessions <- allSessions db
        facebookProfiles <- allFacebookProfiles db
        googleProfiles <- allGoogleProfiles db
        internalProfiles <- allInternalProfiles db
        profiles <- allProfiles db
        userProfiles <- allUserProfiles db

        printf "Number of users:        %5d\n" $ length users
        printf "Number of sessions:     %5d\n" $ length sessions
        printf "Number of profiles:     %5d\n" $ length profiles
        printf "Number of regions:      %5d\n" $ length regions
        printf "Number of coordinates:  %5d\n" $ length geoCoords
        printf "Number of real estates: %5d\n" $ length realEstates
        printf "Number of bookings:     %5d\n" $ length bookings

        putStrLn "Inserting Data"
  
        runSqlite (pack targetDbPath) $ do
          runMigration migrateAll

          insertAll users
          insertAll sessions
          insertAll facebookProfiles
          insertAll googleProfiles
          insertAll internalProfiles
          insertAll profiles
          insertAll regions
          insertAll geoCoords
          insertAll realEstates
          insertAll bookings
          insertAll allCategories
          mapM_ insert userProfiles -- don't care about surrogate key of the junction table

          createIndexes
