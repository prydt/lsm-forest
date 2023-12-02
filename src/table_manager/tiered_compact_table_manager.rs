use crate::lsm_forest::LogSerial;
use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::vec;

use crate::table_manager::simple_table_manager::*;
use crate::table_manager::TableManager;

pub struct TieredCompactTableManager<K: LogSerial, V: LogSerial> {
    pub tm: SimpleTableManager<K, V>,
    pub level2: Vec<PathBuf>,
    pub level3: Option<PathBuf>,
    pub compact_threshold: usize,
}

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for TieredCompactTableManager<K, V> {
    fn new(p: &Path) -> Self {
        let mut level2 = Vec::new();
        let mut level3 = None;

        for file in fs::read_dir(p).unwrap() {
            let file = file.unwrap();
            let path = file.path();

            match path.extension() {
                Some(ext) => {
                    if ext == "sst2" {
                        level2.push(path);
                    } else if ext == "sst3" {
                        level3 = Some(path);
                    }
                }
                None => {}
            }
        }
        level2.sort();

        TieredCompactTableManager::<K, V> {
            tm: SimpleTableManager::<K, V>::new(p),
            level2,
            level3,
            compact_threshold: 5, // DEFAULT of 5
        }
    }

    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
        self.tm.add_table(memtable)?;
        if self.tm.sstables.len() >= self.compact_threshold {
            self.compact()?;
        }
        self.level2.sort();

        Ok(())
    }

    fn read(&mut self, key: &K) -> Option<V> {
        let result = self
            .search_files(self.tm.sstables.clone(), key)
            .or(self.search_files(self.level2.clone(), key))
            .or({
                if let Some(ref level3_path) = self.level3 {
                    self.search_files(vec![level3_path.clone()], key)
                } else {
                    None
                }
            });

        match result {
            Some(value) => value,
            None => None,
        }
    }

    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool {
        self.tm.should_flush(wal, memtable)
    }
}

impl<K: LogSerial, V: LogSerial> TieredCompactTableManager<K, V> {
    fn compact(&mut self) -> Result<()> {
        let mut compact_table = BTreeMap::new();
        let mut name = format!("sstable_{:08}.sst2", self.level2.len());

        if self.level2.len() >= self.compact_threshold {
            name = "sstable_00000000.sst3".to_string();
            match self.level3 {
                Some(ref level3_path) => {
                    let f = File::open(&level3_path).unwrap();
                    let mut reader = std::io::BufReader::new(&f);

                    while let Ok(entry) =
                        bincode::decode_from_reader::<
                            SimpleTableEntry<K, V>,
                            &mut std::io::BufReader<&File>,
                            _,
                        >(&mut reader, bincode::config::standard())
                    {
                        compact_table.insert(entry.key, entry.value);
                    }

                    fs::remove_file(&level3_path)?;
                }
                None => {}
            }

            self.level2.sort();
            for table in self.level2.iter() {
                let f = File::open(table).unwrap();
                let mut reader = std::io::BufReader::new(&f);

                while let Ok(entry) = bincode::decode_from_reader::<
                    SimpleTableEntry<K, V>,
                    &mut std::io::BufReader<&File>,
                    _,
                >(&mut reader, bincode::config::standard())
                {
                    compact_table.insert(entry.key, entry.value);
                }

                fs::remove_file(table)?;
            }
            self.level2.clear();
        }

        self.tm.sstables.sort();
        for table in self.tm.sstables.iter() {
            let f = File::open(table).unwrap();
            let mut reader = std::io::BufReader::new(&f);

            while let Ok(entry) = bincode::decode_from_reader::<
                SimpleTableEntry<K, V>,
                &mut std::io::BufReader<&File>,
                _,
            >(&mut reader, bincode::config::standard())
            {
                compact_table.insert(entry.key, entry.value);
            }

            fs::remove_file(table)?;
        }
        self.tm.sstables.clear();

        let path = self.tm.path.join(&name);
        if name.ends_with("2") {
            self.level2.push(path.clone());
            self.level2.sort();
        } else {
            self.level3 = Some(path.clone());
        }

        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        for (key, value) in compact_table {
            let entry = SimpleTableEntry { key, value };
            let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
            file.write(&payload)?;
        }
        file.flush()?;

        Ok(())
    }

    fn search_files(&mut self, mut files: Vec<PathBuf>, key: &K) -> Option<Option<V>> {
        files.sort();
        for path in files.iter().rev() {
            let f = File::open(path).unwrap();
            let mut reader = std::io::BufReader::new(&f);
            while let Ok(entry) = bincode::decode_from_reader::<
                SimpleTableEntry<K, V>,
                &mut std::io::BufReader<&File>,
                _,
            >(&mut reader, bincode::config::standard())
            {
                if entry.key == key.clone() {
                    return Some(entry.value);
                } else if entry.key > key.clone() {
                    break;
                }
            }
        }
        None
    }
}
