{-# LANGUAGE TemplateHaskell #-}
module Types where

import           Control.Applicative    (empty, (<$>), pure)
import           Data.Aeson
import           Data.Aeson.TH          (deriveJSON, defaultOptions)
import qualified Data.ByteString.Char8  as C
import qualified Data.Text              as T

data Edge = Edge
  {  from  :: URL
    ,to    :: URL
  } deriving (Show)

newtype TaskId = TaskId { fromTaskId :: String } deriving (Show, Eq, Ord)
newtype URL = URL { fromURL :: String } deriving (Show, Eq, Ord)

instance FromJSON TaskId where
  parseJSON = withText "String" $ (pure . TaskId . T.unpack)

instance ToJSON TaskId where
  toJSON (TaskId str) = String $ T.pack str

instance FromJSON URL where
  parseJSON = withText "String" $ (pure . URL . T.unpack)

instance ToJSON URL where
  toJSON (URL str) = String $ T.pack str

