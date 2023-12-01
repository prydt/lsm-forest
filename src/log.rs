use crate::lsm_forest::LogSerial;
use anyhow::Result;
use bincode::{Decode, Encode};
use core::fmt::Debug;
use crc32fast;
use std::io::Write;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::BufReader,
    path::Path,
    path::PathBuf,
};

pub struct Log {
    pub file: File,
    pub path: PathBuf,
}

#[derive(Encode, Decode, Debug)]
pub struct LogEntry<K: LogSerial, V: LogSerial> {
    pub crc: u32,
    // pub is_delete: bool,
    pub key: K,
    pub value: V,
}

impl<K: LogSerial, V: LogSerial> LogEntry<K, Option<V>> {
    pub fn compute_crc(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        let mut h = DefaultHasher::new();
        // self.is_delete.hash(&mut h);
        self.key.hash(&mut h);
        self.value.hash(&mut h);
        hasher.update(&h.finish().to_le_bytes());

        hasher.finalize()
    }

    pub fn check_crc(&self) -> bool {
        self.crc == self.compute_crc()
    }

    pub fn set_crc(&mut self) {
        self.crc = self.compute_crc();
    }
}

impl Log {
    pub fn new(path: &Path) -> Log {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();

        Log { file, path: path.to_path_buf() }
    }

    pub fn append<K: LogSerial, V: LogSerial>(
        &mut self,
        entry: LogEntry<K, Option<V>>,
    ) -> Result<()> {
        let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
        self.file.write(&payload)?;
        self.file.flush()?;

        Ok(())
    }

    pub fn recovery<K: LogSerial, V: LogSerial>(&mut self) -> Result<BTreeMap<K, Option<V>>> {
        let mut reader = BufReader::new(&self.file);

        let mut memtable = BTreeMap::new();

        while let Ok(entry) = bincode::decode_from_reader::<
            LogEntry<K, Option<V>>,
            &mut BufReader<&File>,
            _,
        >(&mut reader, bincode::config::standard())
        {
            if entry.check_crc() {
                memtable.insert(entry.key, entry.value);
            }
        }

        Ok(memtable)
    }

    pub fn clear(&mut self) -> Result<()> {
        // self.file.set_len(0)?;
        // fs::remove_file(self.file.path)?;
        // self.file.flush()?;
        // self.file.sync_all()?;
        fs::remove_file(&self.path)?;
        
        self.file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.path)
            .unwrap();

        assert_eq!(self.file.metadata()?.len(), 0);
        Ok(())
    }
}
