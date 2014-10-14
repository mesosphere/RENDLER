{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Scheduler
import           Types

import           Control.Applicative
import           Control.Concurrent
import           Control.Exception      as E
import qualified Data.ByteString.Char8  as C
import           Data.IORef
import           Data.Map.Strict        as Map
import           Data.Monoid            ((<>))
import qualified Data.Set               as Set
import           System.Posix.Signals
import           System.Environment
import           System.Exit
import           System.Mesos.Scheduler
import           System.Mesos.Types

main = do
  (seedUrl, master) <- args
  printIntro seedUrl master
  let info = (frameworkInfo "" "RENDLER") { frameworkRole = Just "*" }
  scheduler <- RendlerScheduler <$>
                newIORef False <*>
                newIORef [URL seedUrl] <*>
                newIORef [URL seedUrl] <*>
                newIORef Set.empty <*>
                newIORef [] <*>
                newIORef Map.empty <*>
                newIORef 0 <*>
                newIORef 0

  status <- withSchedulerDriver scheduler info (C.pack master) Nothing $ \d -> do
    start d
    installHandler keyboardSignal (Catch (shutdownRendler d scheduler)) Nothing
    await d
  if status /= Stopped
    then do
      exitFailure
    else do
      exitSuccess
  where
    args :: IO (String, String)
    args = do
      arguments <- getArgs
      if (length arguments) /= 2 then do
        printUsage
        exitFailure
      else do
        let seedUrl = arguments !! 0
        let master = arguments !! 1
        return (seedUrl, master)

printUsage = do
  putStrLn "Usage:"
  putStrLn "  run <seed-url> <mesos-master>"
  putStrLn ""

printIntro seedUrl master = do
  putStrLn ""
  putStrLn "RENDLER"
  putStrLn "======="
  putStrLn ""
  putStrLn $ "    seedURL: [" <> seedUrl <> "]"
  putStrLn $ "mesosMaster: [" <> master <> "]"
  putStrLn ""

shutdownRendler :: SchedulerDriver -> RendlerScheduler -> IO ()
shutdownRendler driver s = do
  atomicModifyIORef (shutdown s) (\_ -> (True, ()))
  putStrLn "Shutdown initiated. Waiting for all tasks to complete..."
  awaitTasks
  stop driver False
  return ()
  where
    awaitTasks = do
      running <- readIORef (runningTasks s)
      if running > 0 then do
        threadDelay (500 * 1000)
        awaitTasks
      else return ()
