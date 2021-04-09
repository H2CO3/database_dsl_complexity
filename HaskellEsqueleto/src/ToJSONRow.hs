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

module ToJSONRow where

import Data.Text (Text)
import Data.List (sortBy)
import qualified Data.Vector as V
import qualified Data.HashMap.Lazy as H
import Data.Aeson (ToJSON, toJSON, FromJSON, Value(Null, Bool, Number, String, Array, Object))

-- Convert statically-typed tuples to dynamically-typed name-value pairs
class ToJSONRow a b where
  toJSONRow :: a -> b -> [(Text, Value)]

instance ToJSONRow () () where
  toJSONRow () () = []

instance (ToJSON a) => ToJSONRow (Text) (a) where
  toJSONRow (n0) (v0) = [(n0, toJSON v0)]

instance (ToJSON a, ToJSON b) => ToJSONRow (Text, Text) (a, b) where
  toJSONRow (n0, n1) (v0, v1) = [(n0, toJSON v0), (n1, toJSON v1)]

instance (ToJSON a, ToJSON b, ToJSON c) => ToJSONRow (Text, Text, Text) (a, b, c) where
  toJSONRow (n0, n1, n2) (v0, v1, v2) =
    [ (n0, toJSON v0)
    , (n1, toJSON v1)
    , (n2, toJSON v2)
    ]

instance (ToJSON a, ToJSON b, ToJSON c, ToJSON d) =>
  ToJSONRow (Text, Text, Text, Text) (a, b, c, d) where
    toJSONRow (n0, n1, n2, n3) (v0, v1, v2, v3) =
      [ (n0, toJSON v0)
      , (n1, toJSON v1)
      , (n2, toJSON v2)
      , (n3, toJSON v3)
      ]

instance (ToJSON a, ToJSON b, ToJSON c, ToJSON d, ToJSON e) =>
  ToJSONRow (Text, Text, Text, Text, Text) (a, b, c, d, e) where
    toJSONRow (n0, n1, n2, n3, n4) (v0, v1, v2, v3, v4) =
      [ (n0, toJSON v0)
      , (n1, toJSON v1)
      , (n2, toJSON v2)
      , (n3, toJSON v3)
      , (n4, toJSON v4)
      ]

instance (ToJSON a, ToJSON b, ToJSON c, ToJSON d, ToJSON e, ToJSON f) =>
  ToJSONRow (Text, Text, Text, Text, Text, Text) (a, b, c, d, e, f) where
    toJSONRow (n0, n1, n2, n3, n4, n5) (v0, v1, v2, v3, v4, v5) =
      [ (n0, toJSON v0)
      , (n1, toJSON v1)
      , (n2, toJSON v2)
      , (n3, toJSON v3)
      , (n4, toJSON v4)
      , (n5, toJSON v5)
      ]

instance (ToJSON a, ToJSON b, ToJSON c, ToJSON d, ToJSON e, ToJSON f, ToJSON g) =>
  ToJSONRow (Text, Text, Text, Text, Text, Text, Text) (a, b, c, d, e, f, g) where
    toJSONRow (n0, n1, n2, n3, n4, n5, n6) (v0, v1, v2, v3, v4, v5, v6) =
      [ (n0, toJSON v0)
      , (n1, toJSON v1)
      , (n2, toJSON v2)
      , (n3, toJSON v3)
      , (n4, toJSON v4)
      , (n5, toJSON v5)
      , (n6, toJSON v6)
      ]

instance (ToJSON a, ToJSON b, ToJSON c, ToJSON d, ToJSON e, ToJSON f, ToJSON g, ToJSON h) =>
  ToJSONRow (Text, Text, Text, Text, Text, Text, Text, Text) (a, b, c, d, e, f, g, h) where
    toJSONRow (n0, n1, n2, n3, n4, n5, n6, n7) (v0, v1, v2, v3, v4, v5, v6, v7) =
      [ (n0, toJSON v0)
      , (n1, toJSON v1)
      , (n2, toJSON v2)
      , (n3, toJSON v3)
      , (n4, toJSON v4)
      , (n5, toJSON v5)
      , (n6, toJSON v6)
      , (n7, toJSON v7)
      ]

-- Approximate comparison
(~==) :: Value -> Value -> Bool
(~==) Null Null = True
(~==) (Bool x) (Bool y) = x == y
(~==) (Number x) (Number y) = abs (x - y) <= 1e-9
(~==) (String x) (String y) = x == y
(~==) (Array xs) (Array ys) = (&&) sameLength $ foldl (&&) True $ map eq xys
  where
    sameLength = (length xs) == (length ys)
    eq (x, y) = x ~== y
    xys = (V.toList xs) `zip` (V.toList ys)
(~==) (Object xs) (Object ys) = (&&) sameLength $ foldl (&&) True $ map eq xys
  where
    eq ((kx, vx), (ky, vy)) = kx == ky && vx ~== vy
    sameLength = (length xs) == (length ys)
    xys = (sortByFst $ H.toList xs) `zip` (sortByFst $ H.toList ys)
    sortByFst = sortBy $ \(k1, _) (k2, _) -> compare k1 k2
(~==) _ _ = False

-- The same, for lists
(~==.) :: [Value] -> [Value] -> Bool
(~==.) xs ys = foldl (&&) True $ map eq $ xs `zip` ys
  where
    eq (x, y) = x ~== y
