use crate::{log::*, table_manager::SimpleTableManager, table_manager::TableManager};
use anyhow::Result;
use bincode::{Decode, Encode};
use core::fmt::Debug;
use crc32fast;
use std::io::Write;
use std::path::PathBuf;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::BufReader,
    path::Path,
};

pub trait LogSerial = Encode + Decode + Hash + Ord + 'static + Debug + Clone;

pub struct LSMTree<
    'a,
    K: LogSerial,
    V: LogSerial,
    TM: TableManager<K, V> = SimpleTableManager<K, V>,
> {
    pub path: PathBuf,
    pub wal: Log,
    pub memtable: BTreeMap<K, Option<V>>,
    pub table_manager: &'a mut TM,
}

/// LSM Tree
impl<'a, K: LogSerial, V: LogSerial, TM: TableManager<K, V>> LSMTree<'a, K, V, TM> {
    /// Create a new LSM Tree
    pub fn new(p: PathBuf, tm: &'a mut TM) -> LSMTree<'a, K, V, TM> {
        // TODO add recovery
        LSMTree {
            path: p.clone(),
            wal: Log::new(&p.join("wal.log")),
            memtable: BTreeMap::new(),
            table_manager: tm,
        }
    }

    // TODO TEST
    pub fn get(&self, key: &K) -> Option<V> {
        // look at memtable
        match self.memtable.get(&key) {
            Some(value) => value.clone(),
            None => self.table_manager.read(&key),
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        // add to memtable
        let mut log_entry = LogEntry {
            crc: 0,
            key: key.clone(),
            value: Some(value.clone()),
        };
        log_entry.set_crc();
        self.wal.append(log_entry)?;
        self.memtable.insert(key, Some(value));

        if self.table_manager.should_flush(&self.wal, &self.memtable) {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: K) {
        todo!()
    }

    // TODO TEST??
    pub fn flush_memtable(&mut self) -> Result<()> {
        // flush memtable to disk
        self.table_manager.add_table(self.memtable.clone())?;
        self.memtable.clear();
        self.wal = Log::new(&self.path.join("wal.log"));
        Ok(())
    }
}
