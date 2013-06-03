-- | Core module for Haskoop Configurations
module Haskoop.Configuration where
import Haskoop.Configuration.Types
import Control.Monad(liftM)
import Control.Monad.State

-- | A Configuration is a State Monad
type Configuration a = State JobConfiguration a

-- | Getter and Setters for the State
getInputs :: Configuration [String]
getInputs = get >>= liftM input

