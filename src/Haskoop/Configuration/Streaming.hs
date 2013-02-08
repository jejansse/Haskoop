-- |Module that contains the mapping from Haskoop configuration options to
-- Hadoop Streaming
{-# LANGUAGE OverloadedStrings #-}
module Haskoop.Configuration.Streaming where
import Haskoop.Configuration.Types
import Data.List (intercalate)

class StreamingArg a where
	streamingArg :: a -> String

-- FIXME: we should only do one intercalate and just gather all the other stuff in a big list.
instance StreamingArg JobConfiguration where
	streamingArg jc = intercalate " " [inputOption, outputOption, streamingGenericOptions, streamingOptionalOptions]
		where 
			inputOption = intercalate " " ["-input " ++ f | f <- input jc]
			outputOption = "-output " ++ output jc
			streamingGenericOptions = intercalate " " (map streamingArg (genericOptions jc))
			streamingOptionalOptions = intercalate " " (map streamingArg (optionalOptions jc))

instance StreamingArg OptionalOption where
	streamingArg (File f) = "-file " ++ f
	streamingArg (InputFormat i) = "-inputformat " ++ i
	streamingArg (OutputFormat o) = "-outputformat " ++ o
	streamingArg (Partitioner p) = "-partitioner " ++ p
	streamingArg (CmdEnv e v) = "-cmdenv " ++ (show e) ++ "=" ++ (show v)
	streamingArg (InputReader i) = "-inputreader " ++ i
	streamingArg (Background False) = ""
	streamingArg (Background True) = "-background"
	streamingArg (Verbose False) = ""
	streamingArg (Verbose True) = "-verbose"
	streamingArg (LazyOutput False) = ""
	streamingArg (LazyOutput True) = "-lazyoutput"
	streamingArg (NumReduceTasks n) = "-numreducetasks " ++ (show n)
	streamingArg (MapDebug d) = "-mapdebug " ++ d
	streamingArg (ReduceDebug d) = "-reducedebug " ++ d
	streamingArg (IO s) = "-io " ++ s

instance StreamingArg GenericOption where
	streamingArg (Conf c) = "-conf " ++ c
	streamingArg (Property e v) = "-D " ++ (show e) ++ "=" ++ (show v)
	streamingArg (FileSystem LocalHost) = "-fs localhost"
	streamingArg (FileSystem (RemoteHost host port)) = "-fs " ++ (show host) ++ ":" ++ (show port)
	streamingArg (JobTracker LocalHost) = "-jt localhost"
	streamingArg (JobTracker (RemoteHost host port)) = "-jt " ++ (show host) ++ ":" ++ (show port)
	streamingArg (Files fs) = "-files " ++ (intercalate "," fs)
	streamingArg (LibJars jars) = "-libjars " ++ (intercalate "," jars)
	streamingArg (Archives as) = "-archives " ++ (intercalate "," as)
