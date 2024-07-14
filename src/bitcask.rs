#![deny(missing_docs)]
use std::collections::HashMap;
use std::fs::File;
use crate::{KVStore, Result};

const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

/// `StoreCommand` specifies the operation on a key.
enum StoreCommand {
    Set(String),
    Remove,
}

/// `BitcaskEntry` represents a single entry in a bitcask data file.
/// The entries are sorted by timestamp and the value depends on the
/// command specified.
struct BitcaskEntry {
    crc: u16,
    ts: u128,
    ksz: u32,
    vsz: u32,
    key: String,
    value: StoreCommand,
}

impl BitcaskEntry {
    fn get_value(&self) -> Option<String> {
        match &self.value {
            StoreCommand::Remove => None,
            StoreCommand::Set(ref val) => Some(val.clone()),
        }
    }
}

#[derive(Clone, Copy)]
struct BitcaskPtr {
    file_id: u128,
    vsz: u32,
    vpos: usize,
    ts: u128,
}

/// `BitcaskStore` represents a key-value store backed by the Bitcask
/// algorithm. Internally, this is a Log-sorted merge tree optimised
/// for the write throughput.
pub struct BitcaskStore {
    data_directory: String,
    current_open_file: String,
    key_dir: HashMap<String, BitcaskPtr>,
}

impl BitcaskStore {
    /// `open` creates a new `BitcaskStore` instance with the data
    /// from the data directory. It also runs the initial recovery
    /// procedure to get the fetch the data from the disk after a
    /// crash or a restart.
    pub fn open(data_directory: String) -> Result<BitcaskStore> {
        let mut store = BitcaskStore{
            data_directory,
            current_open_file: "".to_string(),
            key_dir: HashMap::new(),
        };
        BitcaskStore::recover_from_disk(&mut store)?;
        Ok(store)
    }

    fn recover_from_disk(store: &mut BitcaskStore) -> Result<()> {
        todo!()
    }

    fn fetch_entry(&self, bitcask_ptr: BitcaskPtr) -> Result<BitcaskEntry> {
        todo!()
    }

    fn append_command_to_store(&mut self, key: &str, cmd: StoreCommand) -> Result<BitcaskPtr> {
        todo!()
    }

    fn update_key_directory(&mut self, key: String, bitcask_ptr: BitcaskPtr) {
        self.key_dir.insert(key, bitcask_ptr);
    }

    fn remove_entry_from_key_directory(&mut self, key: String) {
        self.key_dir.remove(&key);
    }
}

impl KVStore for BitcaskStore {
    type Key = String;
    type Value = String;

    fn get(&self, key: Self::Key) -> Result<Option<Self::Value>> {
        let entry = self.key_dir.get(&key);
        match entry {
            Some(bitcask_ptr) => {
                self.fetch_entry(*bitcask_ptr)
                    .map(|bentry| bentry.get_value())
            },
            None => Ok(None),
        }
    }

    fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        let set_cmd = StoreCommand::Set(value);
        let bitcask_ptr = self.append_command_to_store(&key, set_cmd)?;
        self.update_key_directory(key, bitcask_ptr);
        Ok(())
    }

    fn remove(&mut self, key: Self::Key) -> Result<()> {
        let remove_cmd = StoreCommand::Remove;
        let bitcask_ptr = self.append_command_to_store(&key, remove_cmd)?;
        self.remove_entry_from_key_directory(key);
        Ok(())
    }
}