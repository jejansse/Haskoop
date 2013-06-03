-- |Module containing all main Haskoop types.
module Haskoop.Types where

-- |A 'KeyValue' is any type that can be used as a key or as a value.
class (Show a, Read a, Eq a) => KeyValue a where
	-- |Read the value from a string.
	readKeyValue :: String -> a
	readKeyValue = read
	-- |Transform the value into a string.
	showKeyValue :: a -> String
	showKeyValue = show
instance KeyValue String where
	readKeyValue s = s
	showKeyValue s = s
instance KeyValue Int where
	readKeyValue = read
	showKeyValue = show

-- |A 'Mapper' is a function mapping a key-value pair to a list of key-value pairs in the Haskoop monad.
type Mapper k1 v1 k2 v2 = k1 -> v1 -> IO [(k2,v2)]

-- |A 'Reducer' is a function mapping a key and list of values to a list of key-values pairs in the Haskoop monad.
type Reducer k1 v1 k2 v2 = k1 -> [v1] -> IO [(k2,v2)]

-- |An 'Iteration' is a combination of a 'Mapper' and a 'Reducer'.
data Iteration k1 v1 k3 v3 = forall k2 v2 . (KeyValue k2, KeyValue v2) => Iteration (Mapper k1 v1 k2 v2) (Reducer k2 v2 k3 v3)

-- |A 'Job' is either empty or a list of iterations.
data Job k1 v1 k3 v3 = SingleJob (Iteration k1 v1 k3 v3) | forall k2 v2 . (KeyValue k2, KeyValue v2) => ConsJob (Iteration k1 v1 k2 v2) (Job k2 v2 k3 v3)

-- |The 'iteration' function transforms a 'Mapper' and a 'Reducer' in a 'Job'.
iteration :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2, KeyValue k3, KeyValue v3)
		  => Mapper k1 v1 k2 v2 -> Reducer k2 v2 k3 v3 -> Job k1 v1 k3 v3
iteration m r = SingleJob (Iteration m r)

-- |The '(>>>)' function is a convenient shorthand for ConsJob.
(>>>) :: (KeyValue k1, KeyValue v1, KeyValue k2, KeyValue v2, KeyValue k3, KeyValue v3)
      => Iteration k1 v1 k2 v2 -> Job k2 v2 k3 v3 -> Job k1 v1 k3 v3
(>>>) = ConsJob

data TaskState = TaskState {
	jobConfiguration :: JobConfiguration
}

-- Should be able to read the TaskConfiguration, but what State do we have?
type Mapper st k1 v1 k2 v2 = RWST TaskConfiguration w st IO [(k2,v2)]
