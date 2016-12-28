{-# Language TemplateHaskell #-}
{-# Language DeriveGeneric #-}
{-# Language DeriveDataTypeable #-}
{-# Language BangPatterns #-}
{-# Language ScopedTypeVariables #-}
{-# Language GADTs #-}
{-# Language RankNTypes #-}

module Main where

import Control.Concurrent
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Text.Printf

import Lib


task1Def :: TaskDef
task1Def = defTask "task1" (\ctx -> threadDelay 4000000 >> print ctx >> return Success) []

task1 :: Context -> Process ()
task1 = makeTask task1Def


task2Def :: TaskDef
task2Def = defShellTask "task2" (const "ls not-a-file") [task1Def]

task2 :: Context -> Process ()
task2 = makeTask task2Def


task3Def :: TaskDef
task3Def = defShellTask "task3" (printf "echo %s" . show . recv) [task1Def]

task3 :: Context -> Process ()
task3 = makeTask task3Def


remotable ['task1, 'task2, 'task3]

taskList :: TaskList
taskList = [ (task1Def, $(mkClosure 'task1))
           , (task2Def, $(mkClosure 'task2))
           , (task3Def, $(mkClosure 'task3))
           ]


main :: IO ()
main = distMain taskList Main.__remoteTable
