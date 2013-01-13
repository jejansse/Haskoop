{-# LANGUAGE ExistentialQuantification #-}
module Haskoop where
import Data.List

type Mapper k1 v1 k2 v2 = k1 -> v1 -> [(k2,v2)]
type Reducer k1 v1 k2 v2 = k1 -> [v1] -> [(k2,v2)]
data Iteration k1 v1 k3 v3 = forall k2 v2 . Iteration {
	mapper :: Mapper k1 v1 k2 v2,
	reducer :: Reducer k2 v2 k3 v3
}
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
	mappers = undefined,
	reducers = undefined,
	jobType = undefined,
	number = undefined
}

runPlainIteration :: Configuration -> Iteration k1 v1 k2 v2 -> IO ()
runPlainIteration = undefined
runHadoopIteration :: Configuration -> Iteration k1 v1 k2 v2 -> IO ()
runHadoopIteration = undefined

runIteration :: Configuration -> Iteration k1 v1 k2 v2 -> IO ()
runIteration conf@(Configuration { jobType = Plain }) it = runPlainIteration conf it
runIteration conf@(Configuration { jobType = Hadoop}) it = runHadoopIteration conf it 

newtype Job k1 v1 k2 v2 = Job { runJob :: Configuration -> IO ()}

job ::  Iteration k1 v1 k2 v2 -> Job k1 v1 k2 v2
job it = Job $ \conf -> runIteration conf it

concatJob :: Job k1 v1 k2 v2 -> Job k2 v2 k3 v3 -> Job k1 v1 k3 v3
concatJob j1 j2 = Job $ \conf -> do
	runJob j1 (conf { output = intermediateFile conf, number = number conf })
	runJob j2(conf { input = [intermediateFile conf], number = number conf + 1 })
  where
  	intermediateFile conf = output conf ++ makeIntermediateSuffix (output conf) (number conf)
  	makeIntermediateSuffix file jobNumber = if hasIntermediateSuffix file then "" else "_pre" ++ show jobNumber

-- Checks for "_pre1" etc. at the end of the intermediate file
hasIntermediateSuffix :: String -> Bool
hasIntermediateSuffix s = tail (reverse "_pre") `isPrefixOf` reverse s