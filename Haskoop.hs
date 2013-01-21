{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Haskoop where
import Control.Arrow

type Mapper k1 v1 k2 v2 = k1 -> v1 -> IO [(k2,v2)]

type Reducer k1 v1 k2 v2 = k1 -> [v1] -> IO [(k2,v2)]

data Iteration k1 v1 k3 v3 = forall k2 v2 . (Showable k2, Ord k2, Showable v2, Ord v2) => Iteration {
	mapper :: Mapper k1 v1 k2 v2,
	reducer :: Reducer k2 v2 k3 v3
}

type IterationNumber  = Int

data JobType = Plain | Hadoop

data Configuration = Configuration {
	input :: Maybe [String],
	output :: Maybe String,
	mappers :: Maybe Int,
	reducers :: Maybe Int
}

defaultConfiguration :: Configuration
defaultConfiguration = Configuration {
	input = Nothing,
	output = Nothing,
	mappers = Just 1,
	reducers = Just 1
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


runMapper :: (Parsable k1, Parsable v1, Showable v2, Showable k2) => Mapper k1 v1 k2 v2 -> IO ()
runMapper mapper = do
	contents <- getContents
	mapOutput <- mapM (uncurry mapper . parseKeyValue) $ lines contents
	mapM_ putStrLn $ map showKeyValue $ concat mapOutput


runReducer :: (Parsable k1, Parsable v1, Eq k1, Eq v1, Showable v2, Showable k2) => Reducer k1 v1 k2 v2 -> IO ()
runReducer reducer = do
	contents <- getContents
	reduceOutput <- mapM (uncurry reducer) $ groupMapOutput $ map parseKeyValue $ lines contents
	mapM_ putStrLn $ map showKeyValue $ concat reduceOutput


--runPlainIteration :: (Parsable k1, Parsable v1, Showable k3, Ord k3, Showable v3) => Configuration -> Iteration k1 v1 k3 v3 -> IO ()
--runPlainIteration conf (Iteration mapper reducer) = do
--	contents <- getContents
--	mapM_ putStrLn $ map showKeyValue $ concatMap (uncurry reducer) $ groupMapOutput $ L.sort $ concatMap (uncurry mapper . parseKeyValue) $ lines contents


parseKeyValue :: (Parsable k, Parsable v) => String -> (k,v)
parseKeyValue = (parse *** parse) . split (== '\t')


split :: (a -> Bool) -> [a] -> ([a], [a])
split p xs = let (group, rest) = break p xs
			 in (group, tail rest)


showKeyValue :: (Showable k, Showable v) => (k,v) -> String
showKeyValue (k,v) = showit k ++ "\t" ++ showit v


runHadoopIteration :: Configuration -> Iteration k1 v1 k2 v2 -> IO ()
runHadoopIteration conf (Iteration mapper reducer) = undefined


-- Groups the streaming reduce output per key. This is guaranteed to be sorted by key.
-- FIXME: use something more efficient than list concatenation.
-- FIXME: use Data.Vector and use span and break!!!
groupMapOutput :: (Eq k, Eq v) => [(k,v)] -> [(k,[v])]
groupMapOutput kvs = groupMapOutput' kvs []
groupMapOutput' kvs gs
	| kvs == [] = gs
	| otherwise = groupMapOutput' rest (gs ++ [(k, map snd group)])
		where 
			k = fst (head kvs)
			(group, rest) = span ((== k) . fst) kvs


--runIteration :: (Parsable k1, Parsable v1, Showable k3, Ord k3, Showable v3) => Configuration -> Iteration k1 v1 k3 v3 -> IO ()
--runIteration conf@(Configuration { jobType = Just Plain }) it = runPlainIteration conf it
--runIteration conf@(Configuration { jobType = Just Hadoop}) it = runHadoopIteration conf it




--runHaskoop :: Job k1 v1 k2 v2 -> IO ()
--runHaskoop job = do
--	args <- getArgs
--	runHaskoop' job args

--runHaskoop' :: Job k1 v1 k2 v2 -> IO ()
--runHaskoop' job [] = runJob job
--runHaskoop' job ("map":args) = runMapper 
--runHaskoop' job ("reduce":args) = undefined


--concatJob :: Job k1 v1 k2 v2 -> Job k2 v2 k3 v3 -> Job k1 v1 k3 v3
--concatJob j1 j2 = Job $ \conf -> do
--	runJob j1 (conf { output = intermediateFile conf, number = number conf })
--	runJob j2 (conf { input = [intermediateFile conf], number = number conf + 1 })
--  where
--  	intermediateFile conf = output conf ++ makeIntermediateSuffix (output conf) (number conf)
--  	makeIntermediateSuffix file jobNumber = if hasIntermediateSuffix file then "" else "_pre" ++ show jobNumber

-- Checks for "_pre1" etc. at the end of the intermediate file
--hasIntermediateSuffix :: String -> Bool
--hasIntermediateSuffix s = tail (reverse "_pre") `isPrefixOf` reverse s