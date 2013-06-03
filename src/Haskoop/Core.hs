{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Haskoop.Core where
import Control.Arrow
import System.Environment
import System.Process
import Data.Maybe (fromJust)
import Data.List (intersperse)
import Haskoop.Configuration.Types
import Haskoop.Configuration.Streaming
import Haskoop.Types

-- |A 'KeyValue' is any type that can be used as a key or as a value.
class (Show a, Read a, Eq a) => KeyValue a where
	readKeyValue :: String -> a
	readKeyValue = read
	showKeyValue :: a -> String
	showKeyValue = show
instance KeyValue String where
	readKeyValue s = s
	showKeyValue s = s
instance KeyValue Int where
	readKeyValue = read
	showKeyValue = show

-- |The 'job' function creates a job from a mapper and a reducer.
job :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2, KeyValue k3, KeyValue v3)
  => Mapper k1 v1 k2 v2 -> Reducer k2 v2 k3 v3 -> Job k1 v1 k3 v3
job m r = ConsJob (Iteration m r) EmptyJob

-- |The '(>>>)' function is a convenient shorthand for ConsJob.
(>>>) :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2, KeyValue k3, KeyValue v3)
      => Iteration k1 v1 k2 v2 -> Job k2 v2 k3 v3 -> Job k1 v1 k3 v3
(>>>) = ConsJob

-- |The 'runHaskoop' function is the main function for any Haskoop program.
runHaskoop :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2)
           => JobConfiguration -> Job k1 v1 k2 v2 -> IO ()
runHaskoop conf j = do
	args <- getArgs
	progName <- getProgName
	if null args 
		then runJob progName conf j 
		else parseAndRunJobIteration j args


parseAndRunJobIteration :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2) 
						=> Job k1 v1 k2 v2 -> [String] -> IO ()
parseAndRunJobIteration j ("map":[i]) = runJobIteration j (read i) IterationMapper
parseAndRunJobIteration j ("reduce":[i]) = runJobIteration j (read i) IterationReducer
parseAndRunJobIteration _ _ = error "Wrong arguments, expecting \"map\" or \"reduce\""

type ProgramName = String

runJob :: ProgramName -> JobConfiguration -> Job k1 v1 k2 v2 -> IO ()
runJob progName conf job = go job 0
	where 
		go :: Job k1 v1 k2 v2 -> Int -> IO ()
		go EmptyJob _ = return ()
		go (ConsJob _ j) i = do
			system $ hadoopBinary ++ " dfs -rmr " ++ outputFile
			system $ hadoopBinary ++ " jar " ++ streamingJar' ++ " " ++ (streamingArgs i)
			go j (i+1)
		hadoopBinary = fromJust (hadoop conf)
		streamingJar' = fromJust (streamingJar conf)
		streamingArgs i = concat $ intersperse " " [inputArgs, outputArg, progArg, mapArg i, reduceArg i]
		inputArgs = concat $ intersperse " " ["-input " ++ inputFile | inputFile <- fromJust (input conf)]
		outputFile = fromJust (output conf)
		outputArg = "-output " ++ outputFile
		progArg = "-file " ++ progName
		mapArg i = "-mapper " ++ "\'./" ++ progName ++ " map " ++ show i ++ "\'"
		reduceArg i = "-reducer " ++ "\'./" ++ progName ++ " reduce " ++ show i ++ "\'"


type IterationNumber = Int
data IterationPhase = IterationMapper | IterationReducer


runJobIteration :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2) 
				=> Job k1 v1 k2 v2 -> IterationNumber -> IterationPhase -> IO ()
runJobIteration EmptyJob _ _ = return ()
runJobIteration (ConsJob (Iteration m _) _) 0 IterationMapper = runMapper m
runJobIteration (ConsJob (Iteration _ r) _) 0 IterationReducer = runReducer r
runJobIteration (ConsJob _ j) i p = runJobIteration j (i-1) p


runMapper :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2)
		  => Mapper k1 v1 k2 v2 -> IO ()
runMapper mapper = do
	contents <- getContents
	mapOutput <- mapM (uncurry mapper . parseKeyValue) $ lines contents
	mapM_ putStrLn $ map printKeyValue $ concat mapOutput


-- TODO: implement the above function in a better way and integrate
-- the implementation with runreducer somehow
runMapper mapper = getContents >>= runMapper' >>= mapM_ putStrLn
    where runMapper' = concatMapM (uncurry mapper . parseRecord . lines) >>= lift (map printKeyValue)


runReducer :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2)
           => Reducer k1 v1 k2 v2 -> IO ()
runReducer reducer = do
	contents <- getContents
	reduceOutput <- mapM (uncurry reducer) $ groupReduceInput $ map parseKeyValue $ lines contents
	mapM_ putStrLn $ map printKeyValue $ concat reduceOutput


-- Call this parseRecord for better naming
parseKeyValue :: (KeyValue k, KeyValue v) => String -> (k,v)
parseKeyValue = (readKeyValue *** readKeyValue) . split (== '\t')


concatMapM :: Monad m => m [[a]] -> m [a]
concatMapM = liftM concat . mapM


split :: (a -> Bool) -> [a] -> ([a], [a])
split p xs = (group, tail rest)
	where (group, rest) = break p xs


printKeyValue :: (KeyValue k, KeyValue v) => (k,v) -> String
printKeyValue (k,v) = showKeyValue k ++ "\t" ++ showKeyValue v

-- |The 'groupReduceInput' function groups together all lines that Hadoop
-- Streaming
-- that Hadoop Streaming inputs to our 
groupReduceInput :: (Eq k, Eq v) => [(k,v)] -> [(k,[v])]
groupReduceInput [] = []
groupReduceInput ((k,v):xs) = (k, v : map snd ys) : groupReduceInput zs
    where (ys,zs) = span ((== k) . fst) xs
