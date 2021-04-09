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

module Index where

import Data.Text (pack)
import Data.List (intercalate)
import Text.Printf (printf)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (ReaderT)
import Database.Persist.Sql

-- Unfortunately, Persistent doesn't create indexes automatically
-- for foreign keys. So, we need to create them manually.
createIndexes :: MonadIO m => ReaderT SqlBackend m ()
createIndexes = do
  -- createIndex True  "user" ["username"] -- already handled by the uniqueness constraint
  createIndex False "user" ["birth_date"]

  -- createIndex True "user_profile" ["profile"] -- already handled by the uniqueness constraint
  createIndex False "user_profile" ["user"]

  createIndex False "profile" ["facebook"]
  createIndex False "profile" ["google"]
  createIndex False "profile" ["internal"]

  createIndex False "session" ["user"]
  createIndex False "session" ["logout"]

  createIndex False "auth_event" ["pub_key"]

  createIndex False "real_estate" ["owner"]
  createIndex False "real_estate" ["region"]

  createIndex False "region" ["parent"]

  createIndex False "geo_coord" ["region"]

  createIndex False "booking" ["real_estate"]
  createIndex False "booking" ["user"]

createIndex :: MonadIO m => Bool -> String -> [String] ->ReaderT SqlBackend m ()
createIndex isUnique table columns = do
  let indexName = intercalate "_" $ table : columns
  let commaSepCols = intercalate ", " columns
  let unique :: String = if isUnique then "UNIQUE" else ""
  let sql = pack $ printf "CREATE %s INDEX IF NOT EXISTS %s ON %s(%s)" unique indexName table commaSepCols
  _ :: [Single PersistValue] <- rawSql sql []
  return ()
