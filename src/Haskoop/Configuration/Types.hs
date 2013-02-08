-- |Module that contains the configuration types for Haskoop.
module Haskoop.Configuration.Types where

data JobConfiguration = JobConfiguration {
	input :: [String],
	output :: String,
	hadoop :: String,
	streamingJar :: String,
	optionalOptions :: [OptionalOption],
	genericOptions :: [GenericOption]
}

data OptionalOption
	= File String
	| InputFormat String
	| OutputFormat String
	| Partitioner String
	| CmdEnv String String
	| InputReader String
	| Background Bool
	| Verbose Bool
	| LazyOutput Bool
	| NumReduceTasks Int
	| MapDebug String
	| ReduceDebug String
	| IO String

data GenericOption
	= Conf String
	| Property String String
	| FileSystem HostName
	| JobTracker HostName
	| Files [String]
	| LibJars [String]
	| Archives [String]

type Host = String
type Port = Int

data HostName = LocalHost | RemoteHost Host Port

