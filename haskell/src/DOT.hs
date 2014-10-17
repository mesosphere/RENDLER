{-# LANGUAGE OverloadedStrings #-}
module DOT where

import Types

import           Control.Monad
import           Data.Digest.Pure.SHA
import qualified Data.ByteString.Char8  as C
import qualified Data.ByteString.Lazy   as L
import           Data.List
import qualified Data.Map.Strict        as M
import           Data.Maybe
import           Data.Monoid            ((<>))
import qualified Data.Set               as Set
import           System.IO
import           System.Path            (absNormPath)
import           System.Directory       (getCurrentDirectory)

writeDOTFile :: [Edge] -> M.Map URL URL -> FilePath -> IO ()
writeDOTFile edges images outFile = do
  file <- openFile outFile WriteMode
  dir <- getCurrentDirectory
  let absFilePath = fromJust $ absNormPath dir outFile
  putStrLn $ "Writing results to [" <> absFilePath <> "]"

  hPutStrLn file "digraph G {"
  hPutStrLn file "  node [shape=box];"

  maybeHashedUrls <- forM (M.toList images) $ \((URL url), (URL imageUrl)) -> do
    if isPrefixOf "file:///" imageUrl then do
      let fileName = drop 8 imageUrl
      let hashedURL = hashURL url
      C.hPutStrLn file $ hashedURL <> "[label=\"\" image=\"" <> C.pack fileName <> "\"];"
      return (Just hashedURL)
    else return Nothing

  let hashedUrls = Set.fromList $ mapMaybe id maybeHashedUrls
  forM_ edges $ \(Edge (URL from) (URL to)) -> do
    let fromHash = hashURL $ from
    let toHash = hashURL $ to
    if (Set.member fromHash hashedUrls) && (Set.member toHash hashedUrls) then do
      putStrLn $ from <> " -> " <> to
      C.hPutStrLn file $ "  " <> fromHash <> " -> " <> toHash
    else return ()

  hPutStrLn file "}"
  hClose file
  putStrLn $ "Wrote results to [" <> absFilePath <> "]"
  where
    hashURL :: String -> C.ByteString
    hashURL url =  "X" <> C.pack (showDigest (sha256 (L.fromStrict (C.pack url))))
