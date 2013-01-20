{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OverloadedStrings #-}
module Haskoop where
import Control.Arrow
import Control.Monad
import qualified Data.List as L

type Mapper k1 v1 k2 v2 = k1 -> v1 -> [(k2,v2)]

type Reducer k1 v1 k2 v2 = k1 -> [v1] -> [(k2,v2)]

data Iteration k1 v1 k3 v3 = forall k2 v2 . (Show k2, Ord k2, Show v2) => Iteration {
	mapper :: Mapper k1 v1 k2 v2,
	reducer :: Reducer k2 v2 k3 v3
}

newtype Job k1 v1 k2 v2 = Job { runJob :: Configuration -> IO ()}

data JobType = Plain | Hadoop

data Configuration = Configuration {
	input :: Maybe [String],
	output :: Maybe String,
	mappers :: Maybe Int,
	reducers :: Maybe Int,
	jobType :: Maybe JobType,
	number :: Maybe Int
}

defaultConfiguration :: Configuration
defaultConfiguration = Configuration {
	input = Nothing,
	output = Nothing,
	mappers = Just 1,
	reducers = Just 1,
	jobType = Just Plain,
	number = Just 0
}

class Parsable a where
	parse :: String -> a

instance Parsable Int where
	parse = read -- FIXME: can we avoid the unpack and read??

instance Parsable String where
	parse s = s


--runPlainIteration :: (Parsable k1, Parsable v1, Show k3, Ord k3, Show v3) => Configuration -> Iteration k1 v1 k3 v3 -> IO ()
--runPlainIteration conf (Iteration mapper reducer) = do
--	contents <- getContents
--	mapM_ putStrLn $ concatMap (showKeyValue . uncurry reducer) $ groupMapOutput $ L.sort $ concatMap (uncurry mapper . parseKeyValue) $ lines contents


parseKeyValue :: (Parsable k, Parsable v) => String -> (k,v)
parseKeyValue = (parse *** parse) . split (== '\t')

split p xs = let (group, rest) = break p xs
			 in (group, tail rest)


showKeyValue :: (Show k, Show v) => (k,v) -> String
showKeyValue (k,v) = show k ++ "\t" ++ show v


--runHadoopIteration :: Configuration -> Iteration k1 v1 k2 v2 -> IO ()
--runHadoopIteration conf (Iteration mapper reducer) = undefined


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


--runIteration :: (Parsable k1, Parsable v1, Show k3, Ord k3, Show v3) => Configuration -> Iteration k1 v1 k3 v3 -> IO ()
--runIteration conf@(Configuration { jobType = Just Plain }) it = runPlainIteration conf it
--runIteration conf@(Configuration { jobType = Just Hadoop}) it = runHadoopIteration conf it 

--job :: (Parsable k1, Parsable v1, Show k3, Show v3) => Iteration k1 v1 k3 v3 -> Job k1 v1 k3 v3
--job it = Job $ \conf -> runIteration conf it

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