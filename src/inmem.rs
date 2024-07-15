//! This module defines a simple in-memory key value store

#![deny(missing_docs)]
use std::collections::HashMap;

use crate::kv::KVStore;
use crate::error::Result;

/// The `InMemoryKVStore` represents a simple in-memory key value pair.
/// It does not store anything on the disk yet.
///
/// ```rust
/// use kvstore_rs::inmem::InMemoryKVStore;
/// use kvstore_rs::kv::KVStore;
/// let mut kv_store = InMemoryKVStore::new().expect("failed to create kv store");
///
/// // add the key value pair and query it
/// kv_store.set("foo".to_owned(), "bar".to_owned()).expect("failed to set key value");
/// assert_eq!(kv_store.get("foo".to_owned()).unwrap(), Some("bar".to_owned()));
///
/// // query a non-existing key
/// assert_eq!(kv_store.get("jaz".to_owned()).unwrap(), None);
///
/// // remove the key added and query
/// kv_store.remove("foo".to_owned()).expect("failed to remove the key");
/// assert_eq!(kv_store.get("foo".to_owned()).unwrap(), None);
///
/// ```
#[derive(Default)]
pub struct InMemoryKVStore {
    kv: HashMap<String, String>,
}

impl InMemoryKVStore {
    /// Open creates a new `KVStore` instance with the
    /// data defined in the `data_directory`.
    pub fn new() -> Result<InMemoryKVStore> {
        Ok(InMemoryKVStore { kv: HashMap::new() })
    }
}

impl KVStore for InMemoryKVStore {
    type Key = String;
    type Value = String;

    /// Get returns the `value` for the `key` if it exists.
    /// Otherwise, it returns None
    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.kv.get(&key).cloned())
    }

    /// Sets the value for the given `key` to the `value`.
    /// If the key already exists, then the value is overwritten
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.kv.insert(key, value);
        Ok(())
    }

    /// Removes a given key. If the key does not exist,
    /// then this is a no-op
    fn remove(&mut self, key: String) -> Result<()> {
        self.kv.remove(&key);
        Ok(())
    }
}
