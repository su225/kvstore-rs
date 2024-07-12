use std::collections::HashMap;

/// The `KVStore` represents a simple in-memory key value pair.
/// It does not store anything on the disk yet.
#[derive(Default)]
pub struct KVStore {
    kv: HashMap<String, String>,
}

impl KVStore {
    /// Creates a `KVStore`
    pub fn new() -> KVStore {
        KVStore { kv: HashMap::new() }
    }

    /// Sets the value for the given `key` to the `value`.
    /// If the key already exists, then the value is overwritten
    pub fn set(&mut self, key: String, value: String) {
        self.kv.insert(key, value);
    }

    /// Get returns the `value` for the `key` if it exists.
    /// Otherwise, it returns None
    pub fn get(&self, key: String) -> Option<String> {
        self.kv.get(&key).cloned()
    }

    /// Removes a given key. If the key does not exist,
    /// then this is a no-op
    pub fn remove(&mut self, key: String) {
        self.kv.remove(&key);
    }
}