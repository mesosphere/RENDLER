{-# LANGUAGE TemplateHaskell #-}
module Crawler where

import Types
import Data.Aeson.TH (deriveJSON, defaultOptions)

data CrawlResult = CrawlResult
  {  taskId  :: TaskId
    ,url    :: URL
    ,links  :: [URL]
  } deriving (Show)

$(deriveJSON defaultOptions ''CrawlResult)

