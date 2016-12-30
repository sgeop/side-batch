{-# Language DeriveGeneric #-}
{-# Language DeriveDataTypeable #-}
{-# Language ScopedTypeVariables #-}
{-# Language GADTs #-}
{-# Language RankNTypes #-}

module Lib
    ( TaskId
    , TaskDef
    , Context (..)
    , TaskList
    , TaskResult (..)
    , StateReq (..)
    , makeTask
    , defTask
    , defShellTask
    , distMain
    ) where

import System.Environment (getArgs)
import Control.Monad (forM_)
import Control.Arrow (first)
import Control.Concurrent
import Control.Distributed.Process
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Node (initRemoteTable)
import Data.Map.Strict as Map
import Data.Binary (Binary)
import Data.Typeable
import GHC.Generics

import Task
import StateManager

data Env
  = Production
  | Development
  | Test
  deriving (Show, Generic, Typeable)

instance Binary Env


getClosures :: TaskList -> Map TaskId (Context -> Closure (Process ()))
getClosures defs = Map.fromList $ Prelude.map (first taskId) defs


makeTask :: TaskDef -> (Context -> Process ())
makeTask def ctx = do
  say $ "starting task: " ++ show (taskId def)
  result <- liftIO $ run def ctx
  say $ show (recv ctx) ++ "finished with: " ++ show result
  let msg = Finish (taskId def, result)
  say $ "sending " ++ show (recv ctx) ++ " " ++ show msg
  send (recv ctx) msg


taskProc :: ProcessId -> ProcessId -> TaskDef -> Process ()
taskProc masterPid statePid def = do
  pid <- getSelfPid
  send statePid (CheckDeps (pid, taskDepIds def))
  (isReady :: Bool) <- expect
  if isReady
  then send masterPid (taskId def)
  else do say $ "deps not met for: " ++ show (taskId def)
          liftIO $ threadDelay 1000000
          taskProc masterPid statePid def


master :: TaskList -> String -> String -> [NodeId] -> Process ()
master tlist env' date' slaves = do
  liftIO . putStrLn $ "Slaves: " ++ show slaves
  mypid <- getSelfPid
  statePid <- spawnLocal stateManager
  let closures = getClosures tlist
  let context = Context { recv = statePid, env = env', date = date'}
  forM_ tlist (spawnLocal . taskProc mypid statePid . fst)
  forM_ (cycle slaves) (\node -> do
    (tid :: TaskId) <- expect
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
