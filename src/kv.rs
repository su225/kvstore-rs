#![deny(missing_docs)]
use crate::error::Result;

/// `KVStore` represents the basic key-value store implementation
pub trait KVStore {
    /// `Key` is the type of the key
    type Key;

    /// `Value` is the type of the value
    type Value;

    /// `get` returns the key of type Key when it exists or None otherwise.
    /// If there is an error while fetching the key, it is reported as Err.
    fn get(&self, key: Self::Key) -> Result<Option<Self::Value>>;

    /// `set` sets the value of the given key to the given value. If the key
    /// already exists then the value is replaced. If there is any error while
    /// setting the value of the key, then it is returned as an Err.
    fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<()>;

    /// `remove` removes the value of the given key if it exists, or it is a no-op.
    /// If there was any error while removing the key, then it is reported.
    fn remove(&mut self, key: Self::Key) -> Result<()>;
}
