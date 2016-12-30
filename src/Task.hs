{-# Language DeriveGeneric #-}
{-# Language DeriveDataTypeable #-}

module Task where

import Control.Exception as Exp
import Control.Distributed.Process
import Data.Binary (Binary)
import Data.Typeable
import GHC.Generics
import System.Process (callCommand)


newtype TaskId = TaskId String
  deriving (Show, Eq, Ord, Generic, Typeable)

instance Binary TaskId


data TaskDef = TaskDef
  { taskId :: TaskId
  , dependsOn :: [TaskDef]
  } deriving (Show, Generic, Typeable)

instance Binary TaskDef


data Task = Task
  { run :: Context -> IO TaskResult
  , def :: TaskDef
  }


data TaskResult
  = Failure
  | Success
  deriving (Show, Generic, Typeable)

instance Binary TaskResult


data Context = Context
  { recv :: ProcessId
  , env :: String
  , date :: String
  } deriving (Show, Generic, Typeable)

instance Binary Context


type TaskList = [(TaskDef, Context -> Closure (Process()))]


taskDepIds :: TaskDef -> [TaskId]
taskDepIds def' = fmap taskId (dependsOn def')


mkTask :: String -> [TaskDef] -> (Context -> IO TaskResult) -> Task
mkTask taskId' dependsOn' run' = Task
  { run = run'
  , def = TaskDef
    { taskId = TaskId taskId'
    , dependsOn = dependsOn'
    }
  }


-- defShellTask :: String -> (Context -> String) -> [TaskDef] -> TaskDef
-- defShellTask taskId' cmd = defTask taskId' (runCmd . cmd)
--   where
--     runCmd a = (callCommand a >> return Success) `Exp.catch` failure
--
--     failure :: IOException -> IO TaskResult
--     failure = const $ return Failure
