module Main where
import Haskoop

-- Word count example

configuration :: Configuration
configuration = defaultConfiguration {
	input = ["access.log"],
	output = "ipcounts"
}

mapper1 :: Int -> String -> [(String, Int)]
mapper1 _ value = [(w,1) | w <- words value]

reducer1 :: String -> [Int] -> [(String, Int)]
reducer1 key values = [(key, sum values)]

main :: IO ()
main = runIteration configuration $ Iteration mapper1 reducer1