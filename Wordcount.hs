module Main where
import Haskoop

-- Word count example

configuration :: Configuration
configuration = defaultConfiguration {
	input = Just ["access.log"],
	output = Just "ipcounts"
}

mapper1 :: Int -> String -> IO [(String, Int)]
mapper1 _ value = return [(w,1) | w <- words value]

reducer1 :: String -> [Int] -> IO [(String, Int)]
reducer1 key values = return [(key, sum values)]

main :: IO ()
main = runMapper mapper1
--main = runReducer reducer1