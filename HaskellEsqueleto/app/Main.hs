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

module Main where

import Control.Monad (forM)
import Control.Monad.IO.Class (liftIO)
import Text.Printf
import Data.Text (pack)
import Data.Maybe (fromJust)
import qualified Data.Map as M
import Database.Persist.Sqlite hiding ((==.))
import Database.Esqueleto.Experimental as E
import System.Exit
import System.TimeIt (timeItT)

import Util
import Queries


-- Helper for removing some redundancy from executing queries
selectRows query transform params = do
  (elapsed, results) <- (timeItT . select . query) params
  return (elapsed, map transform results)

main :: IO ()
main = do
  -- Read example data, transform it, and place it in the local database using Persistent
  let sourceDbPath = "../ExampleData/example_data_before_migration.sqlite3"
  let targetDbPath = "db.sqlite3"
  populateDatabase sourceDbPath targetDbPath

  -- Read the reference results
  testCases <- readTestCases "../ExampleData/query_results_before_migration.json"

  -- define small helpers
  let testParams name = params $ fromJust $ M.lookup name testCases
  let testResults name = results $ fromJust $ M.lookup name testCases

  putStrLn "Running queries\n"
  runSqlite (pack targetDbPath) $ do
    -- Get the results from each query
    continents <- selectRows queryContinents toRegionResult
      $ testParams "continents"
    siblingsAndParents <- selectRows querySiblingsAndParents toRegionResult
      $ testParams "siblings_and_parents"
    noLoginUsers <- selectRows queryNoLoginUsers toUserIdResult
      $ testParams "no_login_users"
    numValidSessions <- selectRows queryNumValidSessions toSessionCountResult
      $ testParams "num_valid_sessions"
    multiProfileUsers <- selectRows queryMultiProfileUsers toProfileCountResult
      $ testParams "multi_profile_users"
    ownedRealEstateRegionCount <- selectRows queryOwnedRealEstateRegionCount toRegionCountResult
      $ testParams "owned_real_estate_region_count"
    profileCountsByNonGoogleUser <- selectRows queryProfileCountsByNonGoogleUser toNonGoogleResult
      $ testParams "profile_counts_by_non_google_user"
    topNBookedRegionsForUserX <- selectRows queryTopNBookedRegionsForUserX toBookedRegionResult
      $ testParams "top_n_booked_regions_for_user_x"
    northestBookedLatitudeSlow <- selectRows queryNorthestBookedLatitudeSlow toLatitudeResult
      $ testParams "northest_booked_latitude_slow"
    northestBookedLatitudeFast <- selectRows queryNorthestBookedLatitudeFast toLatitudeResult
      $ testParams "northest_booked_latitude_fast"
    avgDailyPriceByUserByBookingLengthCategory <- selectRows queryAvgDailyPriceByUserByBookingLengthCategory toAvgPriceResult
      $ testParams "avg_daily_price_by_user_by_booking_length_category"

    let actualValues =
          [ ("continents", continents)
          , ("siblings_and_parents", siblingsAndParents)
          , ("no_login_users", noLoginUsers)
          , ("num_valid_sessions", numValidSessions)
          , ("multi_profile_users", multiProfileUsers)
          , ("owned_real_estate_region_count", ownedRealEstateRegionCount)
          , ("profile_counts_by_non_google_user", profileCountsByNonGoogleUser)
          , ("avg_daily_price_by_user_by_booking_length_category", avgDailyPriceByUserByBookingLengthCategory)
          , ("top_n_booked_regions_for_user_x", topNBookedRegionsForUserX)
          , ("northest_booked_latitude_slow", northestBookedLatitudeSlow)
          , ("northest_booked_latitude_fast", northestBookedLatitudeFast)
          ]

    let numQueries = length actualValues
    let numTests = M.size testCases

    if numQueries /= numTests
      then liftIO $ do
        printf "There are %d queries but %d tests\n\n" numQueries numTests
        exitFailure
      else
        return ()

    -- Check each result set
    success <- forM actualValues $ \(name, (elapsed, actualRows)) -> do
      let actual = toJSONRows actualRows
      let expected = testResults name
      liftIO $ reportResults name actual expected elapsed

    liftIO $ if all id success
             then exitSuccess
             else exitFailure
