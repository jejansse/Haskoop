module Main where
import Haskoop
import Control.Monad (liftM)

-- Word count example

configuration :: Configuration
configuration = defaultConfiguration {
	input = Just ["file:////Users/jeroenthuis/Workspace/Haskoop/examples/wordcount/words.txt"],
	output = Just "haskoop/wordcounts",
	hadoop = Just "/Users/jeroenthuis/Workspace/hadoop-1.0.3/bin/hadoop",
	streamingJar = Just "/Users/jeroenthuis/Workspace/hadoop-1.0.3/contrib/streaming/hadoop-streaming-1.0.3.jar"
}

mapper1 :: Int -> String -> IO [(String, Int)]
mapper1 _ value = return [(w,1) | w <- words value]

reducer1 :: String -> [Int] -> IO [(String, Int)]
reducer1 key values = return [(key, sum values)]

wcJob :: Job Int String String Int
wcJob = job mapper1 reducer1

main :: IO ()
main = do
	runHaskoop configuration wcJob