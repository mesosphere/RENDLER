{-# LANGUAGE OverloadedStrings #-}
module Scheduler where

import           Types
import qualified Crawler                as Crawl
import qualified Renderer               as Rend

import           Control.Applicative
import           Control.Concurrent
import           Control.Monad
import           Data.Aeson (decodeStrict, encode)
import           Data.Bits
import qualified Data.ByteString.Char8  as C
import           Data.IORef
import           Data.Map.Strict        as Map
import           Data.Maybe
import           Data.Monoid            ((<>))
import           Data.Set (Set)
import qualified Data.Set               as Set
import           System.Directory       (getCurrentDirectory)
import           System.Exit
import           System.Mesos.Resources
import           System.Mesos.Scheduler
import           System.Mesos.Types
import           System.Path            (absNormPath)

cpusPerTask = 0.1
memPerTask = 32.0
requiredResources = [Resource "cpus" (Scalar cpusPerTask) Nothing, Resource "mem" (Scalar memPerTask) Nothing]
crawlerId = "crawler"
rendererId = "renderer"

commandUris = do
  dir <- getCurrentDirectory
  let absoluteUris = fmap (\uri -> absNormPath dir uri) uris
  return $ fmap (\uri -> (CommandURI (C.pack (fromJust uri)) Nothing Nothing)) absoluteUris
  where
    uris = [
      "../render.js",
      "../python/crawl_executor.py",
      "../python/export_dot.py",
      "../python/render_executor.py",
      "../python/results.py",
      "../python/task_state.py"]


crawlExecutorSettings fid uris = e { executorName = Just "Crawler Executor"}
  where e = executorInfo (ExecutorID "crawler") fid (CommandInfo uris Nothing (ShellCommand "python crawl_executor.py") Nothing) requiredResources

renderExecutorSettings fid uris = e { executorName = Just "Render Executor"}
  where e = executorInfo (ExecutorID "renderer") fid (CommandInfo uris Nothing (ShellCommand "python render_executor.py --local") Nothing) requiredResources

data RendlerScheduler = RendlerScheduler
  {  shutdown      :: IORef Bool
    ,crawlQueue    :: IORef [URL]
    ,renderQueue   :: IORef [URL]
    ,processedURLs :: IORef (Set URL)
    ,crawlResults  :: IORef [Edge]
    ,renderResults :: IORef (Map URL URL)
    ,startedTasks  :: IORef Int
    ,runningTasks  :: IORef Int
  }

instance ToScheduler RendlerScheduler where
  registered _ _ _ (MasterInfo _ ip portMaybe _ _) = do
    let port = fromMaybe 5050 portMaybe
    putStrLn $ "Registered with Mesos master [" <> ipString <> ":" <> show port <> "]"
    where
      ipString = do
        let a = ip .&. 255
        let b = ip `shiftR` 8 .&. 255
        let c = ip `shiftR` 16 .&. 255
        let d = ip `shiftR` 24 .&. 255
        show a <> "." <> show b <> "." <> show c <> "." <> show d

  disconnected _ _ = putStrLn "Disconnected from the Mesos master..."

  errorMessage _ _ error = C.putStrLn $ "ERROR: [" <> error <> "]"

  executorLost _ _ executorId _ _ = C.putStrLn $ "EXECUTOR LOST: [" <> fromExecutorID executorId <> "]"

  frameworkMessage s _ executorId _ bytes = do
    C.putStrLn $ "Received a framework message from [" <> fromExecutorID executorId <> "] " <> bytes
    let id = fromExecutorID executorId
    if id == crawlerId then do
      let crawlRes = fromJust (decodeStrict bytes :: Maybe Crawl.CrawlResult)
      addCrawlJobs s crawlRes
    else if id == rendererId then do
      let renderRes = fromJust (decodeStrict bytes :: Maybe Rend.RenderResult)
      addRenderResult s renderRes
    else return ()

  offerRescinded _ _ offerId = C.putStrLn $ "Offer [" <> fromOfferID offerId <> "] has been rescinded"

  resourceOffers s driver offers = do
    printStatistics s
    forM_ offers $ \offer -> do
      putStrLn $ "Got resource offer [" <> show offer <> "]"
      shuttingDown <- readIORef (shutdown s)
      if shuttingDown then do
        declineOffer driver (offerID offer) filters
      else do
        let tasksToStart = quot (maxTasksForOffer offer) 2
        renderUrls <- atomicRemove tasksToStart (renderQueue s)
        crawlUrls <- atomicRemove tasksToStart (crawlQueue s)
        renderTasks <- forM renderUrls $ makeTaskWithId makeRenderTask offer
        crawlTasks <- forM crawlUrls $ makeTaskWithId makeCrawlTask offer

        let tasks = renderTasks <> crawlTasks
        if (length tasks) == 0 then
          declineOffer driver (offerID offer) filters
        else
          launchTasks driver [offerID offer] tasks (Filters Nothing)
    where
      makeTaskWithId :: (C.ByteString -> URL -> [CommandURI] -> Offer -> TaskInfo) -> Offer -> URL -> IO TaskInfo
      makeTaskWithId f offer url = do
        uris <- commandUris
        id <- atomicModifyIORef (startedTasks s) (\i -> (i+1, i))
        return $ f (C.pack (show id)) url uris offer

  statusUpdate s driver status = do
    putStrLn $ "Task " <> show (taskStatusTaskID status) <> " is in state " <> show state
    case state of
      TaskRunning -> atomicModifyIORef (runningTasks s) (\x -> (x+1, ()))
      x | (isTerminal x) -> atomicModifyIORef (runningTasks s) (\x -> (x-1, ()))
        | otherwise -> return ()
      where
        state = taskStatusState status

atomicRemove :: Int -> IORef [a] -> IO ([a])
atomicRemove num r =
  atomicModifyIORef' r $ \q -> do
    let tasks = take num q
    let newQueue = drop num q
    (newQueue, tasks)

addCrawlJobs :: RendlerScheduler -> Crawl.CrawlResult -> IO ()
addCrawlJobs s r = do
  forM_ (Crawl.links r) $ \link -> do
    let edge = Edge (Crawl.url r) link
    putStrLn $ "Appending " <> show edge <> " to crawl results"
    atomicModifyIORef (crawlResults s) (\r -> (edge : r, ()))
    processed <- readIORef (processedURLs s)
    if not (Set.member link processed) then do
      putStrLn $ "Enqueueing [" <> show link <> "]"
      atomicModifyIORef (renderQueue s) (\q -> (link : q, ()))
      atomicModifyIORef (crawlQueue s) (\q -> (link : q, ()))
      atomicModifyIORef (processedURLs s) (\p -> (Set.insert link p, ()))
    else return ()

addRenderResult :: RendlerScheduler -> Rend.RenderResult -> IO ()
addRenderResult s r = do
  putStrLn $ "Appending [" <> show (key, value) <> " to render results"
  atomicModifyIORef (renderResults s) (\r -> (insert key value r, ()))
  where
    key = Rend.url r
    value = Rend.imageUrl r

makeCrawlTask :: C.ByteString -> URL -> [CommandURI] -> Offer -> TaskInfo
makeCrawlTask id url uris offer =
  TaskInfo
    "Crawler task"
    (TaskID $ "crawler_" <> id)
    (offerSlaveID offer)
    requiredResources
    (TaskExecutor $ crawlExecutorSettings (offerFrameworkID offer) uris)
    (Just (C.pack (fromURL url)))
    Nothing
    Nothing

makeRenderTask :: C.ByteString -> URL -> [CommandURI] -> Offer -> TaskInfo
makeRenderTask id url uris offer =
  TaskInfo
    "Render task"
    (TaskID $ "renderer_" <> id)
    (offerSlaveID offer)
    requiredResources
    (TaskExecutor $ renderExecutorSettings (offerFrameworkID offer) uris)
    (Just (C.pack (fromURL url)))
    Nothing
    Nothing

printStatistics :: RendlerScheduler -> IO ()
printStatistics s = do
  putStrLn "Queue Statistics:"
  crawlQ <- readIORef (crawlQueue s)
  putStrLn $ "  Crawl queue length:  [" <> show (length crawlQ) <> "]"
  renderQ <- readIORef (renderQueue s)
  putStrLn $ "  Render queue length: [" <> show (length renderQ) <> "]"
  runningTasks <- readIORef (runningTasks s)
  putStrLn $ "  Running tasks:       [" <> show runningTasks <> "]"

maxTasksForOffer :: Offer -> Int
maxTasksForOffer o =
  floor $ min (cpus / cpusPerTask) (mem / memPerTask)
  where
    (cpus, mem) = cpuAndMemResources o

cpuAndMemResources :: Offer -> (Double, Double)
cpuAndMemResources o =
  Prelude.foldl (\(cpu, mem) resource ->
      case (resourceName resource, resourceValue resource) of
        ("cpus", (Scalar d)) -> (cpu + d, mem)
        ("mem", (Scalar d)) -> (cpu, mem + d)
        _ -> (cpu, mem)
  ) (0.0, 0.0) (offerResources o)

