use crate::lsm_forest::LogSerial;
use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::table_manager::simple_table_manager::SimpleTableManager;
use crate::table_manager::TableManager;

use bloomfilter::Bloom;
use lru::LruCache;
use std::num::NonZeroUsize;

use super::simple_table_manager::SimpleTableEntry;

pub struct SimpleCacheTableManager<K: LogSerial, V: LogSerial> {
    pub tm: SimpleTableManager<K, V>,
    pub cache: LruCache<K, Option<V>>,
}

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for SimpleCacheTableManager<K, V> {
    fn new(p: &Path) -> Self {
        SimpleCacheTableManager::<K, V> {
            tm: SimpleTableManager::<K, V>::new(p),
            cache: LruCache::<K, Option<V>>::new(NonZeroUsize::new(128).unwrap()),
        }
    }

    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
        for (key, value) in memtable.iter() {
            if self.cache.contains(key) {
                self.cache.put(key.clone(), value.clone());
            }
        }

        self.tm.add_table(memtable)
    }

    fn read(&mut self, key: &K) -> Option<V> {
        match self.cache.get(key) {
            Some(value) => value.clone(),
            None => {
                let value = self.tm.read(key);
                self.cache.put(key.clone(), value.clone());
                value
            }
        }
    }

    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool {
        self.tm.should_flush(wal, memtable)
    }
}
