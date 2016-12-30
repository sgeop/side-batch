{-# Language ScopedTypeVariables #-}
{-# Language DeriveGeneric #-}
{-# Language DeriveDataTypeable #-}

module StateManager where

import Control.Distributed.Process
import Control.Monad (filterM)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary)
import Data.Map as Map
import Data.Maybe
import Data.Typeable
import GHC.Generics

import Task

type TaskState = Map.Map TaskId TaskResult
type StateProcess = StateT TaskState Process

data StateReq
  = Finish (TaskId, TaskResult)
  | CheckDeps (ProcessId, [TaskId])
  deriving (Show, Generic, Typeable)

instance Binary StateReq

isFinished :: TaskId -> StateProcess Bool
isFinished tid = do
  taskState <- get
  return (isJust $ Map.lookup tid taskState)

updateState :: TaskId -> TaskResult -> StateProcess ()
updateState tid res = modify $ \s -> insert tid res s


stateManager :: Process ()
stateManager = do
  say "starting state manager"
  evalStateT stateProc Map.empty
    where
      stateProc :: StateProcess ()
      stateProc = do
        (req :: StateReq) <- lift expect
        case req of
          Finish (tid, res) -> updateState tid res
          CheckDeps (pid, deps) -> do
            numFinished <- filterM isFinished deps
            lift $ send pid (length numFinished == length deps)
        stateProc
