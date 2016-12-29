{-# Language ScopedTypeVariables #-}

module JobState where

import Control.Distributed.Process
import Control.Monad (filterM)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import Data.Map as Map
import Data.Maybe

import Lib


type TaskState = Map.Map TaskId TaskResult
type StateProcess = StateT TaskState Process

isFinished :: TaskId -> StateProcess Bool
isFinished tid = do
  taskState <- get
  return (isJust $ Map.lookup tid taskState)

updateState :: TaskId -> TaskResult -> StateProcess ()
updateState tid res = modify $ \s -> insert tid res s


stateManager :: StateProcess ()
stateManager = do
  lift $ say "starting state proc"
  (req :: StateReq) <- lift expect
  case req of
    Finish (tid, res) -> updateState tid res
    CheckDeps (pid, deps) -> do
      numFinished <- filterM isFinished deps
      lift $ send pid (length numFinished == length deps)
  stateManager
