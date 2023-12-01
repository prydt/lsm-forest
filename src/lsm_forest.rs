use crate::{log::*, table_manager::SimpleTableManager, table_manager::TableManager};
use anyhow::Result;
use bincode::{Decode, Encode};
use core::fmt::Debug;
use crc32fast;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};
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
    pub wal: Mutex<Log>,
    pub memtable: RwLock<BTreeMap<K, Option<V>>>,
    pub table_manager: Mutex<&'a mut TM>,
}

/// LSM Tree
impl<'a, K: LogSerial, V: LogSerial, TM: TableManager<K, V>> LSMTree<'a, K, V, TM> {
    /// Create a new LSM Tree
    pub fn new(p: PathBuf, tm: &'a mut TM) -> LSMTree<'a, K, V, TM> {
        let mut log = Log::new(&p.join("wal.log"));

        let memtable = RwLock::new(log.recovery().unwrap_or(BTreeMap::new()));

        LSMTree {
            path: p.clone(),
            wal: Mutex::new(log),
            memtable: memtable,
            table_manager: Mutex::new(tm),
        }
    }

    // TODO TEST
    pub fn get(&self, key: &K) -> Option<V> {
        // look at memtable
        match self.memtable.read().unwrap().get(&key) {
            Some(value) => {
                // println!("memtable hit {:?}: {:?}", key, value.clone().unwrap());
                value.clone()
            }
            None => {
                // println!("memtable miss {:?}", key);
                self.table_manager.lock().unwrap().read(&key)
            }
        }
    }

    fn put_helper(&self, key: K, value: Option<V>) -> Result<()> {
        // add to memtable
        let mut log_entry = LogEntry {
            crc: 0,
            key: key.clone(),
            value: value.clone(),
        };
        log_entry.set_crc();
        {
            self.wal.lock().unwrap().append(log_entry)?;
        }

        {
            self.memtable
                .write()
                .unwrap()
                .insert(key.clone(), value.clone());
        }

        if self
            .table_manager
            .lock()
            .unwrap()
            .should_flush(&self.wal.lock().unwrap(), &self.memtable.read().unwrap())
        {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn put(&self, key: K, value: V) -> Result<()> {
        self.put_helper(key, Some(value))
    }

    pub fn remove(&self, key: &K) -> Result<()> {
        // self.put(key.clone(), None)
        self.put_helper(key.clone(), None)
    }

    // TODO TEST??
    pub fn flush_memtable(&self) -> Result<()> {
        // flush memtable to disk
        {
            self.table_manager
                .lock()
                .unwrap()
                .add_table(self.memtable.read().unwrap().clone())?;
        }

        {
            self.memtable.write().unwrap().clear();
        }

        assert!(self.memtable.read().unwrap().is_empty());
        // self.wal = Log::new(&self.path.join("wal.log"));
        {
            self.wal.lock().unwrap().clear()?;
        }
        assert!(self.wal.lock().unwrap().file.metadata()?.len() == 0);
        Ok(())
    }
}
