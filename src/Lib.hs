{-# Language DeriveGeneric #-}
{-# Language DeriveDataTypeable #-}
{-# Language ScopedTypeVariables #-}
{-# Language GADTs #-}
{-# Language RankNTypes #-}

module Lib
    ( TaskDef
    , Context
    , TaskList
    , TaskResult (..)
    , makeTask
    , defTask
    , distMain
    ) where

import System.Environment (getArgs)
import Control.Monad (forM_)
import Control.Concurrent
import Control.Distributed.Process
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Node (initRemoteTable)
import Data.Map.Strict as Map
import Data.Set as Set
import Data.Binary
import Data.Typeable
import GHC.Generics

data TaskResult = Failure | Success deriving (Show, Generic, Typeable)

data Context = Context { sender :: ProcessId
                       , env :: String
                       , date :: String
                       } deriving (Show, Generic, Typeable)


data TaskDef = TaskDef { taskId :: String
                       , run :: Context -> IO TaskResult
                       , dependsOn :: [TaskDef]
                       }

instance Binary TaskResult
instance Binary Context

type TaskState = MVar (Set.Set String)

type TaskList = [(TaskDef, Context -> Closure (Process()))]

data StateReq = Finish (String, TaskResult)
              | CheckDeps (ProcessId, [String])
              deriving (Show, Generic, Typeable)

instance Binary StateReq


taskDepIds :: TaskDef -> [String]
taskDepIds def = fmap taskId (dependsOn def)


makeTask :: TaskDef -> (Context -> Process ())
makeTask def ctx = do
  say $ "starting task: " ++ show (taskId def)
  result <- liftIO $ run def ctx
  say $ show (sender ctx) ++ "finished with: " ++ show result
  let msg = Finish (taskId def, result)
  say $ "sending " ++ (show $ sender ctx) ++ " " ++ (show msg)
  send (sender ctx) msg


defTask :: String -> (Context -> IO TaskResult) -> [TaskDef] -> TaskDef
defTask taskId' run' dependsOn' = TaskDef { taskId = taskId'
                                          , run = run'
                                          , dependsOn = dependsOn'
                                          }

getClosures :: TaskList -> Map String (Context -> (Closure (Process ())))
getClosures defs = Map.fromList $ Prelude.map (\(a, b) -> (taskId a, b)) defs


stateProc :: (Set String) -> Process ()
stateProc state = do
  say "starting state proc"
  (req :: StateReq) <- expect
  case req of
    Finish (tid, result) -> do
      say $ (show tid) ++ " finished"
      stateProc $ Set.insert tid state
    CheckDeps (pid, deps) -> do
      send pid (all (`Set.member` state) deps)
      say $ (show deps) ++ " not in " ++ (show state)
      stateProc state


taskProc :: ProcessId -> ProcessId -> TaskDef -> Process ()
taskProc masterPid statePid def = do
  pid <- getSelfPid
  send statePid (CheckDeps (pid, taskDepIds def))
  (isReady :: Bool) <- expect
  if isReady
  then send masterPid (taskId def)
  else do say $ "deps not met for: " ++ (taskId def)
          liftIO $ threadDelay 1000000
          taskProc masterPid statePid def


master :: TaskList -> String -> String -> [NodeId] -> Process ()
master tlist env' date' slaves = do
  liftIO . putStrLn $ "Slaves: " ++ show slaves
  mypid <- getSelfPid
  statePid <- spawnLocal $ stateProc Set.empty
  let closures = getClosures tlist
  let context = Context { sender = statePid, env = env', date = date'}
  forM_ tlist (\def -> spawnLocal $ taskProc mypid statePid (fst def))
  forM_ (cycle slaves) (\node -> do
    (tid :: String) <- expect
    spawn node $ (closures ! tid) context)


distMain :: TaskList -> (RemoteTable -> RemoteTable) -> IO ()
distMain tlist rtable = do
  args <- getArgs
  let rt = rtable initRemoteTable
  case args of
    ["master", host, port, env, date] -> do
      backend <- initializeBackend host port rt
      startMaster backend (master tlist env date)

    ["slave", host, port] -> do
      backend <- initializeBackend host port rt
      startSlave backend
