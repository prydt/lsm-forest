use crate::{log::*, table_manager::TableManager};
use anyhow::Result;
use bincode::{Decode, Encode};
use core::fmt::Debug;
use crc32fast;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::BufReader,
    path::Path,
};

use crate::table_manager::simple_table_manager::SimpleTableManager;

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

    pub fn get(&self, key: &K) -> Option<V> {
        // look at memtable
        match self.memtable.read().unwrap().get(&key) {
            Some(value) => value.clone(),
            None => self.table_manager.lock().unwrap().read(&key),
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
            let mut wal_lock = self.wal.lock().unwrap();
            wal_lock.append(log_entry)?;

            self.memtable
                .write()
                .unwrap()
                .insert(key.clone(), value.clone());
        }

        {
            let mut wal_lock = self.wal.lock().unwrap();
            let mut memtable_lock = self.memtable.write().unwrap();
            let mut tm_lock = self.table_manager.lock().unwrap();

            if tm_lock.should_flush(&wal_lock, &&memtable_lock) {
                self.flush_memtable_helper(wal_lock, memtable_lock, tm_lock)?;
            }
        }

        Ok(())
    }

    pub fn put(&self, key: K, value: V) -> Result<()> {
        self.put_helper(key, Some(value))
    }

    pub fn remove(&self, key: &K) -> Result<()> {
        self.put_helper(key.clone(), None)
    }

    fn flush_memtable_helper(
        &self,
        mut wal_lock: MutexGuard<Log>,
        mut memtable_lock: RwLockWriteGuard<BTreeMap<K, Option<V>>>,
        mut tm_lock: MutexGuard<&mut TM>,
    ) -> Result<()> {
        tm_lock.add_table(memtable_lock.clone())?;

        memtable_lock.clear();

        assert!(memtable_lock.is_empty());

        wal_lock.clear()?;
        assert!(wal_lock.file.metadata()?.len() == 0);

        Ok(())
    }

    pub fn flush_memtable(&self) -> Result<()> {
        let mut wal_lock = self.wal.lock().unwrap();
        let mut memtable_lock = self.memtable.write().unwrap();
        let mut tm_lock = self.table_manager.lock().unwrap();

        self.flush_memtable_helper(wal_lock, memtable_lock, tm_lock)
    }
}
