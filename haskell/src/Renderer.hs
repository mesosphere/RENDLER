{-# LANGUAGE TemplateHaskell #-}
module Renderer where

import Types
import Data.Aeson.TH (deriveJSON, defaultOptions)

data RenderResult = RenderResult
  {  taskId   :: TaskId
    ,url      :: URL
    ,imageUrl :: URL
  } deriving (Show)

$(deriveJSON defaultOptions ''RenderResult)

