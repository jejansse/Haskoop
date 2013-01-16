{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Haskoop where
import qualified Data.List as L
import qualified Data.Set as S
import Control.Arrow

type Mapper k1 v1 k2 v2 = k1 -> v1 -> [(k2,v2)]

type Reducer k1 v1 k2 v2 = k1 -> [v1] -> [(k2,v2)]

data Iteration k1 v1 k3 v3 = forall k2 v2 . (Show k2, Ord k2, Show v2) => Iteration {
	mapper :: Mapper k1 v1 k2 v2,
	reducer :: Reducer k2 v2 k3 v3
}

newtype Job k1 v1 k2 v2 = Job { runJob :: Configuration -> IO ()}

data JobType = Plain | Hadoop

data Configuration = Configuration {
	input :: [String],
	output :: String,
	mappers :: Int,
	reducers :: Int,
	jobType :: JobType,
	number :: Int
}

defaultConfiguration :: Configuration
defaultConfiguration = Configuration {
	input = undefined,
	output = undefined,
	mappers = 1,
	reducers = 1,
	jobType = Plain,
	number = 0
}

class Parsable a where
	parse :: String -> a

instance Parsable Int where
	parse = read

instance Parsable String where
	parse s = s

runPlainIteration :: (Parsable k1, Parsable v1, Show k3, Show v3) => Configuration -> Iteration k1 v1 k3 v3 -> IO ()
-- FIXME: we should just read a tab-separate file for key value pairs!
-- FIXME: add the appropriate type constraints
-- Make a temporary file, use that as output and read the other file in line per line
-- and process it using the mapper with the line number as key
runPlainIteration conf (Iteration mapper reducer) = do
	contents <- readFile (head $ input conf)
	let mapOutput = concatMap (uncurry mapper) $ map ((parse *** parse) . break (== '\t')) (lines contents)
	let	keys = S.fromList $ map fst mapOutput
	let getValues k = map snd $ filter ((==k) . fst) mapOutput
	let	reduceInput = [(k, getValues k) | k <- S.elems keys]
	let	reduceOutput = concatMap (uncurry reducer) reduceInput
	mapM_ (putStrLn . (\(k,v) -> show k ++ "\t" ++ show v)) reduceOutput

runHadoopIteration :: Configuration -> Iteration k1 v1 k2 v2 -> IO ()
runHadoopIteration conf it = undefined

runIteration :: (Parsable k1, Parsable v1, Show k3, Show v3) => Configuration -> Iteration k1 v1 k3 v3 -> IO ()
runIteration conf@(Configuration { jobType = Plain }) it = runPlainIteration conf it
runIteration conf@(Configuration { jobType = Hadoop}) it = runHadoopIteration conf it 

job :: (Parsable k1, Parsable v1, Show k3, Show v3) => Iteration k1 v1 k3 v3 -> Job k1 v1 k3 v3
job it = Job $ \conf -> runIteration conf it

concatJob :: Job k1 v1 k2 v2 -> Job k2 v2 k3 v3 -> Job k1 v1 k3 v3
concatJob j1 j2 = Job $ \conf -> do
	runJob j1 (conf { output = intermediateFile conf, number = number conf })
	runJob j2 (conf { input = [intermediateFile conf], number = number conf + 1 })
  where
  	intermediateFile conf = output conf ++ makeIntermediateSuffix (output conf) (number conf)
  	makeIntermediateSuffix file jobNumber = if hasIntermediateSuffix file then "" else "_pre" ++ show jobNumber

-- Checks for "_pre1" etc. at the end of the intermediate file
hasIntermediateSuffix :: String -> Bool
hasIntermediateSuffix s = tail (reverse "_pre") `L.isPrefixOf` reverse s