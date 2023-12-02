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
use super::tiered_compact_table_manager::TieredCompactTableManager;

pub struct BCATTableManager<K: LogSerial, V: LogSerial> {
    pub tm: TieredCompactTableManager<K, V>,
    pub cache: LruCache<K, Option<V>>,
    pub bloom: Bloom<K>,
    pub estimate_max_count: usize,
    pub fp_rate: f64,
}

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for BCATTableManager<K, V> {
    fn new(p: &Path) -> Self {
        let mut level3 = None;
        let mut level2 = Vec::new();
        let mut level1 = Vec::new();

        for file in fs::read_dir(p).unwrap() {
            let file = file.unwrap();
            let path = file.path();

            match path.extension() {
                Some(ext) => {
                    if ext == "sst" {
                        level1.push(path);
                    } else if ext == "sst2" {
                        level2.push(path);
                    } else if ext == "sst3" {
                        level3 = Some(path);
                    }
                }
                None => {}
            }
        }

        level1.sort();
        level2.sort();

        let mut search_files = Vec::new();
        match level3 {
            Some(path) => {
                search_files.push(path);
            }
            None => {}
        }

        search_files.append(&mut level2);
        search_files.append(&mut level1);

        let estimate_max_count = 25000;
        let fp_rate = 0.05;

        let mut memtable = BTreeMap::new();
        let mut bloom = Bloom::new_for_fp_rate(estimate_max_count, fp_rate);

        for path in search_files.iter() {
            let f = File::open(path).unwrap();
            let mut reader = std::io::BufReader::new(&f);
            while let Ok(entry) = bincode::decode_from_reader::<
                SimpleTableEntry<K, V>,
                &mut std::io::BufReader<&File>,
                _,
            >(&mut reader, bincode::config::standard())
            {
                if entry.value != None {
                    memtable.insert(entry.key, entry.value);
                }
            }
        }

        for (key, value) in memtable.iter() {
            match value {
                Some(_) => {
                    bloom.set(key);
                }
                None => {}
            }
        }
        BCATTableManager::<K, V> {
            tm: TieredCompactTableManager::<K, V>::new(p),
            cache: LruCache::<K, Option<V>>::new(NonZeroUsize::new(128).unwrap()),
            bloom,
            estimate_max_count,
            fp_rate,
        }
    }

    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
        for (key, value) in memtable.iter() {
            match value {
                Some(_) => {
                    self.bloom.set(key);
                }
                None => {}
            }
            if self.cache.contains(key) {
                self.cache.put(key.clone(), value.clone());
            }
        }

        self.tm.add_table(memtable)
    }

    fn read(&mut self, key: &K) -> Option<V> {
        if self.bloom.check(key) {
            match self.cache.get(key) {
                Some(value) => value.clone(),
                None => {
                    let value = self.tm.read(key);
                    self.cache.put(key.clone(), value.clone());
                    value
                }
            }
        } else {
            None
        }
    }

    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool {
        self.tm.should_flush(wal, memtable)
    }
}
