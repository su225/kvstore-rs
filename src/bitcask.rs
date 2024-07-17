//! Bitcask implements the bitcask Log-Sorted Merge Tree algorithm
//! like in RiakDB. The specification is defined here
//! https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf

#![deny(missing_docs)]

use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use failure::{Error, format_err};
use glob::glob;
use serde_derive::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::KVStore;

const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
const BITCASK_HEADER_SIZE: u64 = size_of::<BitcaskHeader>() as u64;
const BITCASK_HINT_HEADER_SIZE: usize = size_of::<BitcaskHintHeader>();

// Extensions of files in cask data directory
const BITCASK_DATA_EXTENSION: &'static str = "caskdata";
const BITCASK_HINT_EXTENSION: &'static str = "caskhint";
const BITCASK_WRITING_EXTENSION: &'static str = "caskwriting";

#[derive(Debug, Eq, PartialEq, Hash)]
enum BitcaskFileType {
    Data,
    Hint,
    WriteInProgress,
}

impl BitcaskFileType {
    fn extension(&self) -> &'static str {
        match self {
            BitcaskFileType::Data => BITCASK_DATA_EXTENSION,
            BitcaskFileType::Hint => BITCASK_HINT_EXTENSION,
            BitcaskFileType::WriteInProgress => BITCASK_WRITING_EXTENSION,
        }
    }
}

impl TryFrom<&'_ str> for BitcaskFileType {
    type Error = Error;

    fn try_from(extension: &'_ str) -> Result<Self> {
        match extension {
            BITCASK_DATA_EXTENSION => Ok(BitcaskFileType::Data),
            BITCASK_HINT_EXTENSION => Ok(BitcaskFileType::Hint),
            BITCASK_WRITING_EXTENSION => Ok(BitcaskFileType::WriteInProgress),
            extension => Err(format_err!("unknown extension: {}", extension)),
        }
    }
}

impl Display for BitcaskFileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.extension())
    }
}

/// `BitcaskFileIdentifier` is the structured representation of a filename
/// related to Bitcask.
#[derive(Debug, Eq, PartialEq, Hash)]
struct BitcaskFileIdentifier {
    filetype: BitcaskFileType,
    serial_number: u32,
    create_timestamp: u128,
}

impl TryFrom<PathBuf> for BitcaskFileIdentifier {
    type Error = Error;

    fn try_from(path: PathBuf) -> Result<Self> {
        if path.is_dir() {
            return Err(format_err!("must not be a directory"));
        }
        let filename: &str = path.file_name()
            .map(OsStr::to_str)
            .flatten()
            .ok_or(format_err!("cannot get filename"))?;
        Self::try_from(filename)
    }
}

impl TryFrom<&'_ str> for BitcaskFileIdentifier {
    type Error = Error;

    fn try_from(filename: &'_ str) -> std::result::Result<Self, Self::Error> {
        let file_parts: Vec<&'_ str> = filename.splitn(3, ".").collect();
        if file_parts.len() != 3 {
            return Err(format_err!("invalid filename pattern"));
        }
        let serial_number = file_parts[0].parse::<u32>().map_err(|e| Error::from(e))?;
        let create_timestamp = file_parts[1].parse::<u128>().map_err(|e| Error::from(e))?;
        let filetype = BitcaskFileType::try_from(file_parts[2])?;
        Ok(BitcaskFileIdentifier{serial_number, create_timestamp, filetype })
    }
}

impl Display for BitcaskFileIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.serial_number, self.create_timestamp, self.filetype.extension())
    }
}

/// `BitcaskStoreCommand` specifies the operation on a key.
#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BitcaskStoreCommand {
    Set(String),
    Remove,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct BitcaskHeader {
    crc: u16,
    ts: u128,
    vsz: u64,
    ksz: u64,
}

/// `BitcaskEntry` represents a single entry in a bitcask data file.
/// The entries are sorted by timestamp and the value depends on the
/// command specified. The format is as follows
/// +---------+---------+---------+---------+----------+----------+
/// | crc(2B) | ts(16B) | ksz(8B) | vsz(8B) | key(ksz) | val(vsz) |
/// +---------+---------+---------+---------+----------+----------+
///
/// Notice that until the `key` field, it is all fixed-size fields.
/// Hence, given the start of the entry, we can read the whole entry
/// with the following algorithm. Parse the first 34B which also gives
/// us the key and the value size. We then read the `ksz` and `vsz`
/// bytes and try to interpret it as UTF-8 to recover key and values.
/// We then verify the entire output from `ts` all the way to the
/// end of `val` with the given `crc` to check for corruption.
#[repr(C)]
#[derive(Serialize, Deserialize)]
struct BitcaskEntry {
    header: BitcaskHeader,
    key: String,
    value: BitcaskStoreCommand,
}

impl BitcaskEntry {
    fn parse_keydir_entry(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<(BitcaskHeader, String, u64)> {
        todo!()
    }

    fn get_value(&self) -> Option<String> {
        match &self.value {
            BitcaskStoreCommand::Remove => None,
            BitcaskStoreCommand::Set(ref val) => Some(val.clone()),
        }
    }
}

/// `BitcaskHintHeader` represents the header for the Bitcask hint.
/// The hint is used to quickly recover the in-memory key directory
/// structure. It does not contain the value which means that it is
/// smaller than the data file and hence fewer I/Os on startup.
#[repr(C, packed)]
#[derive(Serialize, Deserialize)]
struct BitcaskHintHeader {
    ts: u128,
    ksz: u32,
    vsz: u64,
    vpos: u64,
}

/// `BitcaskHintEntry` represents a single entry in a bitcask hint file.
/// The entries are sorted by timestamp and are there to aid quick recovery
/// of the bitcask store after a restart.
#[repr(C)]
#[derive(Serialize, Deserialize)]
struct BitcaskHintEntry {
    header: BitcaskHintHeader,
    key: String,
}

impl BitcaskHintEntry {
    fn parse_from(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<Self> {
        let mut hint_header = [0_u8; BITCASK_HINT_HEADER_SIZE];
        cursor.read_exact(&mut hint_header[..])?;
        let header: BitcaskHintHeader = bincode::deserialize(&hint_header)?;

        let mut key_buffer = Vec::with_capacity(header.ksz as usize);
        cursor.read_exact(&mut key_buffer[..])?;

        let key = String::from_utf8(key_buffer).map_err(|e| Error::from(e))?;
        Ok(BitcaskHintEntry{ header, key })
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct BitcaskPtr {
    file_id: u32,
    vsz: u64,
    vpos: u64,
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
        let hint_pattern = format!("{}/*.{}", self.data_directory, BITCASK_HINT_EXTENSION);
        let data_pattern = format!("{}/*.{}", self.data_directory, BITCASK_DATA_EXTENSION);
        let writing_pattern = format!("{}/*.{}", self.data_directory, BITCASK_WRITING_EXTENSION);

        let mut files_to_delete: Vec<PathBuf> = vec![];

        for writing_file_entry in glob(&*writing_pattern)? {

        }

        for hint_file_entry in glob(&*hint_pattern)? {
            // register as hint file
        }

        for data_file_entry in glob(&*data_pattern)? {
            // register as data file
        }

        for to_delete in files_to_delete.into_iter() {

        }

        Ok(())
    }

    fn recover_from_hint_file(&mut self, hint_file_id: u32, path: &PathBuf) -> Result<()> {
        let contents = std::fs::read(path)?;
        let mut cursor = Cursor::new(&contents);
        while cursor.position() < contents.len() as u64 {
            let bitcask_hint = BitcaskHintEntry::parse_from(&mut cursor)?;
            let bitcask_ptr = BitcaskPtr{
                file_id: hint_file_id,
                vsz: bitcask_hint.header.vsz,
                vpos: bitcask_hint.header.vpos,
                ts: bitcask_hint.header.ts,
            };
            self.update_key_directory(bitcask_hint.key, bitcask_ptr);
        }
        Ok(())
    }

    fn recover_from_data_file(&mut self, data_file_id: u32, path: &PathBuf) -> Result<()> {
        let contents = std::fs::read(path)?;
        let mut cursor = Cursor::new(&contents);
        while cursor.position() < contents.len() as u64 {
            let (bitcask_header, bitcask_key, vpos) = BitcaskEntry::parse_keydir_entry(&mut cursor)?;
            let bitcask_ptr = BitcaskPtr{
                file_id: data_file_id,
                vsz: bitcask_header.vsz,
                ts: bitcask_header.ts,
                vpos,
            };
            self.update_key_directory(bitcask_key, bitcask_ptr)
        }
        Ok(())
    }

    fn fetch_value(&self, key: &str, bitcask_ptr: BitcaskPtr) -> Result<String> {
        let ksz = bincode::serialized_size(key)?;
        if bitcask_ptr.vpos < ksz + BITCASK_HEADER_SIZE {
            return Err(format_err!("invalid bitcask_ptr: insufficient bytes for given key size"));
        }
        let entry_offset = bitcask_ptr.vpos - (ksz + BITCASK_HEADER_SIZE);
        self.load_entry_from_datafile(bitcask_ptr.file_id, entry_offset)
    }

    // Loads entry from the data file for a given bitcask pointer and file_id.
    // TODO: cache file handles to avoid opening repeatedly. Currently, it is inefficient
    // TODO: cache the entries so that repeated disk seeks can be avoided.
    // TODO: implement CRC checks to ensure data integrity.
    fn load_entry_from_datafile(&self, file_id: u32, entry_offset: u64) -> Result<String> {
        let datafile_name = format!("{}.{}", file_id, BITCASK_DATA_EXTENSION);
        let mut file_handle = File::open(datafile_name)?;

        let mut entry_header_bytes = [0_u8; BITCASK_HEADER_SIZE as usize];
        file_handle.read_exact_at(entry_header_bytes.borrow_mut(), entry_offset)?;
        let entry_header: BitcaskHeader = bincode::deserialize(entry_header_bytes.borrow())?;

        let val_offset = entry_offset + BITCASK_HEADER_SIZE + entry_header.ksz;
        let mut val_buffer = Vec::with_capacity(entry_header.vsz as usize);

        file_handle.read_exact_at(val_buffer.as_mut(), val_offset)?;
        String::from_utf8(val_buffer).map_err(|e| e.into())
    }

    /// Appends the command to the store. The command acts as a write-ahead logging
    /// system of the data store. The commands are written to the disk and are used
    /// to recover the state of the store on startup.
    fn append_command_to_store(&mut self, key: &str, cmd: BitcaskStoreCommand) -> Result<BitcaskPtr> {
        let cur_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = bincode::serialize(key)?;
        let val = bincode::serialize(&cmd)?;

        let cur_ts_bytes = cur_ts.to_be_bytes();
        let key_size_bytes = (key.len() as u64).to_be_bytes();
        let val_size_bytes = (val.len() as u64).to_be_bytes();

        let mut digest = X25.digest();
        digest.update(&cur_ts_bytes[..]);
        digest.update(&key_size_bytes[..]);
        digest.update(&val_size_bytes[..]);
        digest.update(key.as_ref());
        digest.update(val.as_ref());
        let crc = digest.finalize().to_be_bytes();

        let mut v_offset = 0;
        let mut cur_file = self.current_open_file.as_mut().unwrap();
        v_offset += cur_file.write(&crc[..])?;
        v_offset += cur_file.write(&cur_ts_bytes[..])?;
        v_offset += cur_file.write(&key_size_bytes[..])?;
        v_offset += cur_file.write(&val_size_bytes[..])?;
        v_offset += cur_file.write(key.as_ref())?;
        cur_file.write_all(val.as_ref())?;

        Ok(BitcaskPtr{
            file_id: self.current_serial_number,
            vsz: val.len() as u64,
            vpos: v_offset as u64,
            ts: cur_ts,
        })
    }

    /// Updates the key directory with the bitcask pointer for the given key.
    /// The bitcask pointer is an on-disk pointer to some piece of data. This
    /// is called on inserting/updating some data into the store or during the
    /// recovery after the restart.
    fn update_key_directory(&mut self, key: String, bitcask_ptr: BitcaskPtr) {
        self.key_dir.insert(key, bitcask_ptr);
    }

    /// Removes entry from the in-memory key directory. This is called on
    /// removing an entry from the store. The corresponding action on disk
    /// would be writing a `StoreCommand::Remove` which is effectively a
    /// tombstone on the disk. During the merge, the removal is enforced.
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
                let fetch_res = self.fetch_value(&key, *bitcask_ptr)?;
                Ok(Some(fetch_res))
            },
            None => Ok(None),
        }
    }

    fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        let set_cmd = BitcaskStoreCommand::Set(value);
        let bitcask_ptr = self.append_command_to_store(&key, set_cmd)?;
        self.update_key_directory(key, bitcask_ptr);
        Ok(())
    }

    fn remove(&mut self, key: Self::Key) -> Result<()> {
        let remove_cmd = BitcaskStoreCommand::Remove;
        self.append_command_to_store(&key, remove_cmd)?;
        self.remove_entry_from_key_directory(key);
        Ok(())
    }
}

#[cfg(test)]
mod bitcask_file_identifier_test_suite {
    use super::*;

    #[test]
    fn test_parse_correct_bitcask_datafile_identifier() {
        let caskdata_identifier = "1.102093.caskdata";
        let data_res = BitcaskFileIdentifier::try_from(caskdata_identifier);
        assert_eq!(BitcaskFileIdentifier{
            filetype: BitcaskFileType::Data,
            serial_number: 1,
            create_timestamp: 102093,
        }, data_res.unwrap())
    }

    #[test]
    fn test_parse_correct_bitcask_hintfile_identifier() {
        let caskhint_identifier = "10.298979.caskhint";
        let hint_res = BitcaskFileIdentifier::try_from(caskhint_identifier);
        assert_eq!(BitcaskFileIdentifier{
            filetype: BitcaskFileType::Hint,
            serial_number: 10,
            create_timestamp: 298979,
        }, hint_res.unwrap())
    }

    #[test]
    fn test_parse_correct_bitcask_writing_identifier() {
        let caskwriting_identifier = "21.787977.caskwriting";
        let writing_res = BitcaskFileIdentifier::try_from(caskwriting_identifier);
        assert_eq!(BitcaskFileIdentifier{
            filetype: BitcaskFileType::WriteInProgress,
            serial_number: 21,
            create_timestamp: 787977,
        }, writing_res.unwrap());
    }

    #[test]
    fn test_parse_incorrect_bitcask_file_identifier_more_than_3_parts() {
        let identifier = "20.1989289.20.29798.caskdata";
        let res = BitcaskFileIdentifier::try_from(identifier);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_incorrect_bitcask_file_identifier_fewer_than_3_parts() {
        let identifier = "20.caskdata";
        let res = BitcaskFileIdentifier::try_from(identifier);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_incorrect_bitcask_file_identifier_parts_cannot_be_parsed() {
        let cases = vec![
            ("a.787987.caskdata", "serial number is not integer"),
            ("8768.ab.caskdata", "creation timestamp is not integer"),
            ("9.97987.cask", "unrecognized cask extension"),
        ];
        for (identifier, msg_on_failure) in cases.into_iter() {
            let res = BitcaskFileIdentifier::try_from(identifier);
            assert!(res.is_err(), "{}", msg_on_failure);
        }
    }
}
