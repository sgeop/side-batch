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


task1 :: Task
task1 = mkTask "task1" [] $ \ctx ->
   threadDelay 4000000 >> print ctx >> return Success

runTask1 :: Context -> Process ()
runTask1 = runTask task1


-- task2Def :: TaskDef
-- task2Def = defShellTask "task2" (const "ls not-a-file") [task1Def]
--
-- task2 :: Context -> Process ()
-- task2 = makeTask task2Def
--
--
-- task3Def :: TaskDef
-- task3Def = defShellTask "task3" (printf "echo %s" . show . recv) [task1Def]
--
-- task3 :: Context -> Process ()
-- task3 = makeTask task3Def
--

remotable ['runTask1]

taskList :: TaskList
taskList = [ (def task1, $(mkClosure 'runTask1)) ]


main :: IO ()
main = distMain taskList Main.__remoteTable
