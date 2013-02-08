module Main where
import Haskoop
import Control.Monad (liftM)

-- Word count example

configuration :: JobConfiguration
configuration = JobConfiguration {
	input = ["file:///Users/jeroenthuis/Workspace/Haskoop/examples/wordcount/words.txt"],
	output = "haskoop/wordcounts",
	hadoop = "/Users/jeroenthuis/Workspace/hadoop-1.0.3/bin/hadoop",
	streamingJar = "/Users/jeroenthuis/Workspace/hadoop-1.0.3/contrib/streaming/hadoop-streaming-1.0.3.jar"
}

mapper1 :: Int -> String -> IO [(String, Int)]
mapper1 _ value = return [(w,1) | w <- words value]

reducer1 :: String -> [Int] -> IO [(String, Int)]
reducer1 key values = return [(key, sum values)]

wcJob :: Job Int String String Int
wcJob = iteration mapper1 reducer1

main :: IO ()
main = do
	runHaskoop configuration wcJob