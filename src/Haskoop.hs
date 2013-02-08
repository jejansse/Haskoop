{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Haskoop where
import Control.Arrow
import System.Environment
import System.Process
import Data.Maybe (fromJust)
import Data.List (intersperse)


-- |A 'Mapper' is a function mapping a key-value pair to a list of key-value pairs in the Haskoop monad.
type Mapper k1 v1 k2 v2 = k1 -> v1 -> IO [(k2,v2)]

-- |A 'Reducer' is a function mapping a key and list of values to a list of key-values pairs in the Haskoop monad.
type Reducer k1 v1 k2 v2 = k1 -> [v1] -> IO [(k2,v2)]

-- |An 'Iteration' is a combination of a 'Mapper' and a 'Reducer'.
data Iteration k1 v1 k3 v3 = forall k2 v2 . (KeyValue k2, KeyValue v2) => Iteration (Mapper k1 v1 k2 v2) (Reducer k2 v2 k3 v3)

-- |A 'Job' is either empty or a list of iterations.
data Job k1 v1 k3 v3 = EmptyJob | forall k2 v2 . (KeyValue k2, KeyValue v2) => ConsJob (Iteration k1 v1 k2 v2) (Job k2 v2 k3 v3)

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
           => Configuration -> Job k1 v1 k2 v2 -> IO ()
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

runJob :: ProgramName -> Configuration -> Job k1 v1 k2 v2 -> IO ()
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


data Configuration = Configuration {
	input :: Maybe [String],
	output :: Maybe String,
	mappers :: Maybe Int,
	reducers :: Maybe Int,
	hadoop :: Maybe String,
	streamingJar :: Maybe String
}

defaultConfiguration :: Configuration
defaultConfiguration = Configuration {
	input = Nothing,
	output = Nothing,
	mappers = Just 1,
	reducers = Just 1,
	hadoop = Nothing,
	streamingJar = Nothing
}

runMapper :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2)
		  => Mapper k1 v1 k2 v2 -> IO ()
runMapper mapper = do
	contents <- getContents
	mapOutput <- mapM (uncurry mapper . parseKeyValue) $ lines contents
	mapM_ putStrLn $ map printKeyValue $ concat mapOutput


runReducer :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2)
           => Reducer k1 v1 k2 v2 -> IO ()
runReducer reducer = do
	contents <- getContents
	reduceOutput <- mapM (uncurry reducer) $ groupReduceInput $ map parseKeyValue $ lines contents
	mapM_ putStrLn $ map printKeyValue $ concat reduceOutput


parseKeyValue :: (KeyValue k, KeyValue v) => String -> (k,v)
parseKeyValue = (readKeyValue *** readKeyValue) . split (== '\t')


split :: (a -> Bool) -> [a] -> ([a], [a])
split p xs = let (group, rest) = break p xs
			 in (group, tail rest)


printKeyValue :: (KeyValue k, KeyValue v) => (k,v) -> String
printKeyValue (k,v) = showKeyValue k ++ "\t" ++ showKeyValue v


groupReduceInput :: (Eq k, Eq v) => [(k,v)] -> [(k,[v])]
groupReduceInput [] = []
groupReduceInput ((k,v):xs) = (k, v : map snd ys) : groupReduceInput zs
    where (ys,zs) = span ((== k) . fst) xs
