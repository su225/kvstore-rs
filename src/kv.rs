#![deny(missing_docs)]
use std::collections::HashMap;
use std::path::Path;

use crate::error::Result;

/// The `KVStore` represents a simple in-memory key value pair.
/// It does not store anything on the disk yet.
///
/// ```rust
/// use kvstore_rs::KVStore;
/// let mut kv_store = KVStore::new();
///
/// // add the key value pair and query it
/// kv_store.set("foo".to_owned(), "bar".to_owned());
/// assert_eq!(kv_store.get("foo".to_owned()), Some("bar".to_owned()));
///
/// // query a non-existing key
/// assert_eq!(kv_store.get("jaz".to_owned()), None);
///
/// // remove the key added and query
/// kv_store.remove("foo".to_owned());
/// assert_eq!(kv_store.get("foo".to_owned()), None);
///
/// ```
#[derive(Default)]
pub struct KVStore {
    kv: HashMap<String, String>,
}

impl KVStore {
    /// Open creates a new `KVStore` instance with the
    /// data defined in the `data_directory`.
    pub fn open(data_directory: &Path) -> Result<KVStore> {
        unimplemented!()
    }

    /// Sets the value for the given `key` to the `value`.
    /// If the key already exists, then the value is overwritten
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.kv.insert(key, value);
        Ok(())
    }

    /// Get returns the `value` for the `key` if it exists.
    /// Otherwise, it returns None
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.kv.get(&key).cloned())
    }

    /// Removes a given key. If the key does not exist,
    /// then this is a no-op
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.kv.remove(&key);
        Ok(())
    }
}
