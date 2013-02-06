{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Haskoop where
import Control.Arrow
import System.Environment
import System.Process
import Data.Maybe (fromJust)
import Data.List (intersperse)


type Mapper k1 v1 k2 v2 = k1 -> v1 -> IO [(k2,v2)]

type Reducer k1 v1 k2 v2 = k1 -> [v1] -> IO [(k2,v2)]

data Iteration k1 v1 k3 v3 = forall k2 v2 . (Eq k2, Eq v2, Parsable k2, Parsable v2, Showable k2, Showable v2) => Iteration (Mapper k1 v1 k2 v2) (Reducer k2 v2 k3 v3)

data Job k1 v1 k3 v3 = EmptyJob | forall k2 v2 . (Eq k2, Eq v2, Parsable k2, Parsable v2, Showable k2, Showable v2) => ConsJob (Iteration k1 v1 k2 v2) (Job k2 v2 k3 v3)


-- FIXME: can we get rid of this monstrous type constraint?
job :: (Parsable k1, Parsable v1, Parsable k2, Parsable v2, Eq k2, Eq v2, Parsable k3, Parsable v3, Showable k1,
 Showable v1, Showable k2, Showable v2, Showable k3, Showable v3, Eq k3, Eq v3)
  => Mapper k1 v1 k2 v2 -> Reducer k2 v2 k3 v3 -> Job k1 v1 k3 v3
job m r = ConsJob (Iteration m r) EmptyJob


(>>>) :: (Parsable k1, Parsable v1, Showable k2, Showable v2, Parsable k2, Parsable v2, Eq k2, Eq v2, Showable k3, Showable v3) 
      => Iteration k1 v1 k2 v2 -> Job k2 v2 k3 v3 -> Job k1 v1 k3 v3
(>>>) = ConsJob


runHaskoop :: (Showable k1, Showable v1, Showable k2, Showable v2, Parsable k1, Parsable v1, Parsable k2, Parsable v2) 
           => Configuration -> Job k1 v1 k2 v2 -> IO ()
runHaskoop conf j = do
	args <- getArgs
	progName <- getProgName
	if null args 
		then runJob progName conf j 
		else parseAndRunJobIteration j args


parseAndRunJobIteration :: (Showable k1, Showable v1, Showable k2, Showable v2, Parsable k1, Parsable v1, Parsable k2, Parsable v2) 
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


runJobIteration :: (Parsable k1, Parsable v1, Parsable k2, Parsable v2, Showable k1, Showable v1, Showable k2, Showable v2) 
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

class Parsable a where
	parse :: String -> a

instance Parsable Int where
	parse = read -- FIXME: can we avoid the unpack and read??

instance Parsable String where
	parse s = s

class Showable a where
	showit :: a -> String

instance Showable String where
	showit s = s

instance Showable Int where
	showit = show


runMapper :: (Parsable k1, Parsable v1, Showable v2, Showable k2) 
		  => Mapper k1 v1 k2 v2 -> IO ()
runMapper mapper = do
	contents <- getContents
	mapOutput <- mapM (uncurry mapper . parseKeyValue) $ lines contents
	mapM_ putStrLn $ map showKeyValue $ concat mapOutput


runReducer :: (Parsable k1, Parsable v1, Eq k1, Eq v1, Showable v2, Showable k2) 
           => Reducer k1 v1 k2 v2 -> IO ()
runReducer reducer = do
	contents <- getContents
	reduceOutput <- mapM (uncurry reducer) $ groupMapOutput $ map parseKeyValue $ lines contents
	mapM_ putStrLn $ map showKeyValue $ concat reduceOutput


parseKeyValue :: (Parsable k, Parsable v) => String -> (k,v)
parseKeyValue = (parse *** parse) . split (== '\t')


split :: (a -> Bool) -> [a] -> ([a], [a])
split p xs = let (group, rest) = break p xs
			 in (group, tail rest)


showKeyValue :: (Showable k, Showable v) => (k,v) -> String
showKeyValue (k,v) = showit k ++ "\t" ++ showit v


groupMapOutput :: (Eq k, Eq v) => [(k,v)] -> [(k,[v])]
groupMapOutput kvs = groupMapOutput' kvs []
	where
		groupMapOutput' kvs' gs
			| kvs' == [] = gs
			| otherwise = groupMapOutput' rest (gs ++ [(k, map snd group)])
				where 
					k = fst (head kvs')
					(group, rest) = span ((== k) . fst) kvs'
