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

use super::simple_table_manager::SimpleTableEntry;

pub struct SimpleBloomTableManager<K: LogSerial, V: LogSerial> {
    pub tm: SimpleTableManager<K, V>,
    pub bloom: Bloom<K>,
    pub estimate_max_count: usize,
    pub fp_rate: f64,
}

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for SimpleBloomTableManager<K, V> {
    fn new(p: &Path) -> Self {
        let mut sstables = Vec::new();

        for file in fs::read_dir(p).unwrap() {
            let file = file.unwrap();
            let path = file.path();

            match path.extension() {
                Some(ext) => {
                    if ext == "sst" {
                        sstables.push(path);
                    }
                }
                None => {}
            }
        }

        sstables.sort();

        let estimate_max_count = 25000;
        let fp_rate = 0.05;

        let mut memtable = BTreeMap::new();
        let mut bloom = Bloom::new_for_fp_rate(estimate_max_count, fp_rate);

        for path in sstables.iter() {
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

        SimpleBloomTableManager::<K, V> {
            tm: SimpleTableManager::<K, V>::new(p),
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
        }

        self.tm.add_table(memtable)
    }

    fn read(&mut self, key: &K) -> Option<V> {
        if self.bloom.check(key) {
            self.tm.read(key)
        } else {
            None
        }
    }

    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool {
        self.tm.should_flush(wal, memtable)
    }
}
