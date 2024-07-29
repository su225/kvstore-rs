//! Bitcask implements the bitcask Log-Sorted Merge Tree algorithm
//! like in RiakDB. The specification is defined here
//! https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf

#![deny(missing_docs)]

use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::{File, read};
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

#[derive(Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Copy, Clone)]
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
#[derive(Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Clone, Copy)]
struct BitcaskFileIdentifier {
    serial_number: u32,
    create_timestamp: u128,
    filetype: BitcaskFileType,
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

impl Into<PathBuf> for BitcaskFileIdentifier {
    fn into(self) -> PathBuf {
        format!("{}.{}.{}", self.serial_number, self.create_timestamp, self.filetype.extension()).into()
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

impl BitcaskHeader {
    fn parse_from(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<BitcaskHeader> {
        let mut entry_header = [0_u8; BITCASK_HEADER_SIZE as usize];
        cursor.read_exact(&mut entry_header[..])?;
        let header: BitcaskHeader = bincode::deserialize(&entry_header[..])?;
        Ok(header)
    }
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
    /// `parse_key_dir_entry` parses a key directory entry from the hint or the data
    /// file and returns the necessary data for the key directory to be built.
    fn parse_key_dir_entry(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<(BitcaskHeader, String, u64)> {
        let header = BitcaskHeader::parse_from(cursor)?;
        let mut key_buffer = Vec::with_capacity(header.ksz as usize);
        cursor.read_exact(&mut key_buffer[..])?;

        let key = String::from_utf8(key_buffer).map_err(|e| Error::from(e))?;
        let value_pos = cursor.position();
        Ok((header, key, value_pos))
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
    largest_ts_per_serial_data: HashMap<u32, u128>,
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
            largest_ts_per_serial_data: HashMap::new(),
        };
        store.recover_from_disk()?;
        if store.current_open_file.is_none() {
            panic!("cannot open data file");
        }
        Ok(store)
    }

    /// `recover_from_disk` reads the bitcask data and hint files and recovers the
    /// in-memory key directory structure. It also cleans up any file that should
    /// have been cleaned up, but was not done so due to whatever reason.
    fn recover_from_disk(&mut self) -> Result<()> {
        let data_pattern = format!("{}/*.{}", self.data_directory, BITCASK_DATA_EXTENSION);
        let writing_pattern = format!("{}/*.{}", self.data_directory, BITCASK_WRITING_EXTENSION);

        let mut data_files: BTreeSet<BitcaskFileIdentifier> = BTreeSet::new();

        // First, list all the data files to read from the directory. This
        // includes all the data files that were still being written at the
        // time the process was stopped (or a crash).
        for data_file_entry in glob(&*data_pattern)? {
            let data_file_identifier = BitcaskFileIdentifier::try_from(data_file_entry?)?;
            data_files.insert(data_file_identifier);
        }

        // The data files that are being written have an indicator. So look
        // for all the "in-progress" files and remove the corresponding data
        // files. The half-baked data files and their corresponding hint files
        // will be removed from the data directory. The in-progress files would
        // be written only by the merger which combines several data files into
        // one for compaction (assuming it can be done)
        for writing_file_entry in glob(&*writing_pattern)? {
            let file_identifier = BitcaskFileIdentifier::try_from(writing_file_entry?)?;
            let data_file_identifier = BitcaskFileIdentifier {
                filetype: BitcaskFileType::Data,
                serial_number: file_identifier.serial_number,
                create_timestamp: file_identifier.create_timestamp,
            };
            data_files.remove(&data_file_identifier);
            self.cleanup_bitcask_files(data_file_identifier)?;
        }

        // It is possible that even after successful merging, the old versions of
        // the data files are still present. They should not be used anymore. So
        // we can reclaim some space by deleting them from the data directory. To
        // do that, we need to find out the latest timestamp for each serial number.
        self.largest_ts_per_serial_data = HashMap::new();
        for file_id in data_files.iter() {
            self.largest_ts_per_serial_data.entry(file_id.serial_number)
                .and_modify(|v| { *v = std::cmp::max(file_id.create_timestamp, *v); })
                .or_insert(file_id.create_timestamp);
        }

        // Now run the garbage collection to remove all the files. We can speed up
        // the recovery by moving this to the background. Measure the time taken
        // and decide accordingly. But not make it work!
        data_files.retain(|&file_id| {
            let max_ts = self.largest_ts_per_serial_data.get(&file_id.serial_number).cloned().unwrap();
            debug_assert!(file_id.create_timestamp <= max_ts);
            let should_retain_file = file_id.create_timestamp == max_ts;
            if !should_retain_file {
                let _ = self.cleanup_bitcask_files(file_id.clone());
            }
            should_retain_file
        });

        // We now have the final set of data files after removing the stale files and
        // the in-progress or half-baked files. So read from the rest to recover the
        // state of the "key directory" for the shard.
        for data_file in data_files.into_iter() {
            self.recover_from_single_data_file(data_file)?;
        }

        Ok(())
    }

    /// `recover_from_single_data_file` recovers the key directory entries from a single
    /// bitcask file. It first tries to read it from a hint file because it is much faster
    /// as hint files don't store value. If a hint-file is not present, then it recovers
    /// from the data file. Note that the value is still not read.
    fn recover_from_single_data_file(&mut self, data_file: BitcaskFileIdentifier) -> Result<()> {
        let recovered_from_hint_file = self.try_recover_from_hint_file(&data_file)?;
        if recovered_from_hint_file {
            return Ok(());
        }
        self.do_recover_from_data_file(data_file)
    }

    /// `try_recover_from_hint_file` tries to recover from the hint file for the corresponding
    /// bitcask data file if it exists. If the hint file does not exist, then it returns `Ok(false)`.
    /// If the recovery is successful, then it returns `Ok(true)`. Otherwise, it returns an error.
    fn try_recover_from_hint_file(&mut self, data_file: &BitcaskFileIdentifier) -> Result<bool> {
        let hint_file = self.get_hint_file_for_data_file(&data_file);
        let contents_res = read::<PathBuf>(hint_file.into());
        match contents_res {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::from(e)),
            Ok(contents) => {
                let mut cursor = Cursor::new(&contents);
                while cursor.position() < contents.len() as u64 {
                    let bitcask_hint = BitcaskHintEntry::parse_from(&mut cursor)?;
                    let bitcask_ptr = BitcaskPtr{
                        file_id: hint_file.serial_number,
                        vsz: bitcask_hint.header.vsz,
                        vpos: bitcask_hint.header.vpos,
                        ts: bitcask_hint.header.ts,
                    };
                    self.update_key_directory(bitcask_hint.key, bitcask_ptr);
                }
                Ok(true)
            }
        }
    }

    /// `do_recover_from_data_file` does the actual work of recovering from the data file.
    fn do_recover_from_data_file(&mut self, data_file: BitcaskFileIdentifier) -> Result<()> {
        let contents = read::<PathBuf>(data_file.into())?;
        let mut cursor = Cursor::new(&contents);
        while cursor.position() < contents.len() as u64 {
            let (bitcask_header, bitcask_key, vpos) = BitcaskEntry::parse_key_dir_entry(&mut cursor)?;
            let bitcask_ptr = BitcaskPtr{
                file_id: data_file.serial_number,
                vsz: bitcask_header.vsz,
                ts: bitcask_header.ts,
                vpos,
            };
            self.update_key_directory(bitcask_key, bitcask_ptr)
        }
        Ok(())
    }

    fn cleanup_bitcask_files(&self, data_file: BitcaskFileIdentifier) -> Result<()> {
        debug_assert!(data_file.filetype == BitcaskFileType::Data);
        self.delete_file_if_exists(data_file)?;
        self.delete_file_if_exists(self.get_hint_file_for_data_file(&data_file))?;
        self.delete_file_if_exists(self.get_write_in_progress_file_for_data_file(&data_file))?;
        Ok(())
    }
    
    fn get_hint_file_for_data_file(&self, data_file: &BitcaskFileIdentifier) -> BitcaskFileIdentifier {
        BitcaskFileIdentifier {
            serial_number: data_file.serial_number,
            create_timestamp: data_file.create_timestamp,
            filetype: BitcaskFileType::Hint,
        }
    }

    fn get_write_in_progress_file_for_data_file(&self, data_file: &BitcaskFileIdentifier) -> BitcaskFileIdentifier {
        BitcaskFileIdentifier {
            serial_number: data_file.serial_number,
            create_timestamp: data_file.create_timestamp,
            filetype: BitcaskFileType::WriteInProgress,
        }
    }

    fn delete_file_if_exists(&self, file_id: BitcaskFileIdentifier) -> Result<()> {
        let p: PathBuf = file_id.into();
        match fs::remove_file(p) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(Error::from(e));
            }
            _ => { Ok(()) },
        }
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
