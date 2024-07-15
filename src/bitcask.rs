//! Bitcask implements the bitcask Log-Sorted Merge Tree algorithm
//! like in RiakDB. The specification is defined here
//! https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf

#![deny(missing_docs)]

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use failure::format_err;
use glob::glob;

use crate::error::Result;
use crate::kv::KVStore;

const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

/// `StoreCommand` specifies the operation on a key.
#[derive(Debug, Clone)]
enum StoreCommand {
    Set(String),
    Remove,
}

/// `BitcaskEntry` represents a single entry in a bitcask data file.
/// The entries are sorted by timestamp and the value depends on the
/// command specified.
#[derive(Debug, Clone)]
struct BitcaskEntry {
    crc: u16,
    ts: u128,
    ksz: u32,
    vsz: u32,
    key: String,
    value: StoreCommand,
}

/// `BitcaskHintEntry` represents a single entry in a bitcask hint file.
/// The entries are sorted by timestamp and are there to aid quick recovery
/// of the bitcask store after a restart.
#[derive(Debug, Clone)]
struct BitcaskHintEntry {
    ts: u128,
    ksz: u32,
    vsz: u32,
    vpos: u64,
    key: String,
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
    current_open_file: Option<File>,
    current_serial_number: u32,
    key_dir: HashMap<String, BitcaskPtr>,
}

impl BitcaskStore {
    /// `open` creates a new `BitcaskStore` instance with the data
    /// from the data directory. It also runs the initial recovery
    /// procedure to get the fetch the data from the disk after a
    /// crash or a restart.
    pub fn open(data_directory: &Path) -> Result<BitcaskStore> {
        let mut store = BitcaskStore{
            data_directory: data_directory.to_str().unwrap().to_string(),
            current_open_file: None,
            current_serial_number: 0,
            key_dir: HashMap::new(),
        };
        store.recover_from_disk()?;
        if store.current_open_file.is_none() {
            panic!("cannot open data file");
        }
        Ok(store)
    }

    fn get_serial_number_from_filename(&self, filename: &str) -> Option<u32> {
        todo!()
    }

    fn recover_from_disk(&mut self) -> Result<()> {
        let mut max_serial_number = 0;
        let mut already_processed = HashSet::new();
        let hint_pattern = format!("{}/*.caskdata", self.data_directory);
        let data_pattern = format!("{}/*.caskhint", self.data_directory);

        // First recover from the hint file. This is fast as it does not
        // store the value significantly decreasing the number of bytes to read.
        for hint_file_entry in glob(&*hint_pattern)? {
            match hint_file_entry {
                Ok(hint_file) => {
                    let sr_num = hint_file.file_name()
                        .map(|os_str| os_str.to_str()).flatten()
                        .map(|s| self.get_serial_number_from_filename(s)).flatten();
                    match sr_num {
                        Some(cur_serial_number) => {
                            max_serial_number = max(max_serial_number, cur_serial_number);
                            self.recover_from_hint_file(&hint_file)?;
                            already_processed.insert(cur_serial_number);
                        },
                        None => { return Err(format_err!("invalid hint filename format for {}", hint_file.display())); }
                    }
                },
                Err(e) => { return Err(e.into()); }
            }
        }

        // Then recover from the data file. Skip if we have already read the corresponding
        // hint file. This is the slow path. In the ideal case, most of the key-dir should
        // have been built from the hint files.
        for data_file_entry in glob(&*data_pattern)? {
            match data_file_entry {
                Ok(data_file) => {
                    let sr_num = data_file.file_name()
                        .map(|os_str| os_str.to_str()).unwrap()
                        .map(|s| self.get_serial_number_from_filename(s)).flatten();
                    match sr_num {
                        Some(cur_serial_number) => {
                            if already_processed.contains(&cur_serial_number) {
                                continue;
                            }
                            max_serial_number = max(max_serial_number, cur_serial_number);
                            self.recover_from_data_file(&data_file)?;
                            already_processed.insert(cur_serial_number);
                        },
                        None => { return Err(format_err!("invalid data filename format for {}", data_file.display())); }
                    }
                },
                Err(e) => { return Err(e.into()); }
            }
        }

        // Now, open the current data file.
        let next_serial_number = max_serial_number + 1;
        let cur_open_data_filename = format!("{}.caskdata", next_serial_number);
        let cur_open_datafile = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(cur_open_data_filename)?;

        self.current_serial_number = next_serial_number;
        self.current_open_file = Some(cur_open_datafile);
        Ok(())
    }

    fn recover_from_hint_file(&mut self, path: &PathBuf) -> Result<()> {
        todo!()
    }

    fn recover_from_data_file(&mut self, path: &PathBuf) -> Result<()> {
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
        self.append_command_to_store(&key, remove_cmd)?;
        self.remove_entry_from_key_directory(key);
        Ok(())
    }
}