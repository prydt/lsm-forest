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
            Some(value) => {
                println!("memtable hit {:?}: {:?}", key, value.clone().unwrap());
                value.clone()
            }
            None => {
                println!("memtable miss {:?}", key);
                self.table_manager.read(&key)
            }
        }
    }

    fn put_helper(&mut self, key: K, value: Option<V>) -> Result<()> {
        // add to memtable
        let mut log_entry = LogEntry {
            crc: 0,
            key: key.clone(),
            value: value.clone(),
        };
        log_entry.set_crc();
        self.wal.append(log_entry)?;
        self.memtable.insert(key, value);

        if self.table_manager.should_flush(&self.wal, &self.memtable) {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        self.put_helper(key, Some(value))
    }

    pub fn remove(&mut self, key: &K) -> Result<()> {
        // self.put(key.clone(), None)
        self.put_helper(key.clone(), None)
    }

    // TODO TEST??
    pub fn flush_memtable(&mut self) -> Result<()> {
        // flush memtable to disk
        self.table_manager.add_table(self.memtable.clone())?;
        self.memtable.clear();
        assert!(self.memtable.is_empty());
        self.wal = Log::new(&self.path.join("wal.log"));
        Ok(())
    }
}
