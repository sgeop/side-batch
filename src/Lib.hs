{-# Language DeriveGeneric #-}
{-# Language DeriveDataTypeable #-}
{-# Language ScopedTypeVariables #-}
{-# Language GADTs #-}
{-# Language RankNTypes #-}

module Lib
    ( TaskDef
    , Context (..)
    , TaskList
    , TaskResult (..)
    , makeTask
    , defTask
    , defShellTask
    , distMain
    ) where

import System.Environment (getArgs)
import Control.Monad (forM_)
import Control.Arrow (first)
import Control.Concurrent
import Control.Exception as Exp
import Control.Distributed.Process
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Node (initRemoteTable)
import Data.Map.Strict as Map
import Data.Set as Set
import Data.Binary
import Data.Typeable
import GHC.Generics
import System.Process (callCommand)


data TaskResult
  = Failure
  | Success
  deriving (Show, Generic, Typeable)

instance Binary TaskResult


data Env
  = Production
  | Development
  | Test
  deriving (Show, Generic, Typeable)


data Context = Context
  { recv :: ProcessId
  , env :: String
  , date :: String
  } deriving (Show, Generic, Typeable)

instance Binary Context


data TaskDef = TaskDef
  { taskId :: String
  , run :: Context -> IO TaskResult
  , dependsOn :: [TaskDef]
  }


data StateReq
  = Finish (String, TaskResult)
  | CheckDeps (ProcessId, [String])
  deriving (Show, Generic, Typeable)

instance Binary StateReq


type TaskList = [(TaskDef, Context -> Closure (Process()))]


taskDepIds :: TaskDef -> [String]
taskDepIds def = fmap taskId (dependsOn def)


makeTask :: TaskDef -> (Context -> Process ())
makeTask def ctx = do
  say $ "starting task: " ++ show (taskId def)
  result <- liftIO $ run def ctx
  say $ show (recv ctx) ++ "finished with: " ++ show result
  let msg = Finish (taskId def, result)
  say $ "sending " ++ show (recv ctx) ++ " " ++ show msg
  send (recv ctx) msg


defTask :: String -> (Context -> IO TaskResult) -> [TaskDef] -> TaskDef
defTask taskId' run' dependsOn' =
  TaskDef { taskId = taskId'
          , run = run'
          , dependsOn = dependsOn'
          }


defShellTask :: String -> (Context -> String) -> [TaskDef] -> TaskDef
defShellTask taskId' cmd = defTask taskId' (runCmd . cmd)
  where
    runCmd a = (callCommand a >> return Success) `Exp.catch` failure

    failure :: IOException -> IO TaskResult
    failure = const $ return Failure


getClosures :: TaskList -> Map String (Context -> Closure (Process ()))
getClosures defs = Map.fromList $ Prelude.map (first taskId) defs


stateProc :: Set String -> Process ()
stateProc state = do
  say "starting state proc"
  (req :: StateReq) <- expect
  case req of
    Finish (tid, _) -> do
      say $ show tid ++ " finished"
      stateProc $ Set.insert tid state
    CheckDeps (pid, deps) -> do
      send pid (all (`Set.member` state) deps)
      say $ show deps ++ " not in " ++ show state
      stateProc state


taskProc :: ProcessId -> ProcessId -> TaskDef -> Process ()
taskProc masterPid statePid def = do
  pid <- getSelfPid
  send statePid (CheckDeps (pid, taskDepIds def))
  (isReady :: Bool) <- expect
  if isReady
  then send masterPid (taskId def)
  else do say $ "deps not met for: " ++ taskId def
          liftIO $ threadDelay 1000000
          taskProc masterPid statePid def


master :: TaskList -> String -> String -> [NodeId] -> Process ()
master tlist env' date' slaves = do
  liftIO . putStrLn $ "Slaves: " ++ show slaves
  mypid <- getSelfPid
  statePid <- spawnLocal $ stateProc Set.empty
  let closures = getClosures tlist
  let context = Context { recv = statePid, env = env', date = date'}
  forM_ tlist (spawnLocal . taskProc mypid statePid . fst)
  forM_ (cycle slaves) (\node -> do
    (tid :: String) <- expect
    spawn node $ (closures ! tid) context)


distMain :: TaskList -> (RemoteTable -> RemoteTable) -> IO ()
distMain tlist rtable = do
  args <- getArgs
  let rt = rtable initRemoteTable
  case args of

    ["master", host, port, env', date'] -> do
      backend <- initializeBackend host port rt
      startMaster backend (master tlist env' date')

    ["slave", host, port] -> do
      backend <- initializeBackend host port rt
      startSlave backend

    _ -> print $ "Incorrect options. Input either\n"   ++
                 "master <host> <port> <env> <date>\n" ++
                 "or\n"                                ++
                 "stave <host> <port>"
