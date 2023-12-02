use crate::lsm_forest::LogSerial;
use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::table_manager::simple_table_manager::*;
use crate::table_manager::TableManager;

pub struct SimpleCompactTableManager<K: LogSerial, V: LogSerial> {
    pub tm: SimpleTableManager<K, V>,
    pub compact_threshold: usize,
}

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for SimpleCompactTableManager<K, V> {
    fn new(p: &Path) -> Self {
        SimpleCompactTableManager::<K, V> {
            tm: SimpleTableManager::<K, V>::new(p),
            compact_threshold: 10, // DEFAULT of 10
        }
    }

    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
        self.tm.add_table(memtable)?;
        if self.tm.sstables.len() >= self.compact_threshold {
            self.compact()?;
        }

        Ok(())
    }

    fn read(&mut self, key: &K) -> Option<V> {
        self.tm.read(key)
    }

    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool {
        self.tm.should_flush(wal, memtable)
    }
}

impl<K: LogSerial, V: LogSerial> SimpleCompactTableManager<K, V> {
    fn compact(&mut self) -> Result<()> {
        self.tm.sstables.sort();
        let mut compact_table = BTreeMap::new();

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
        self.tm.add_table(compact_table)?;

        Ok(())
    }
}
