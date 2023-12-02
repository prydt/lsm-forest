use crate::lsm_forest::LogSerial;
use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub mod simple_table_manager;
pub mod simple_compact_table_manager;
pub mod simple_bloom_table_manager;
pub mod simple_cache_table_manager;
pub mod tiered_compact_table_manager;

pub trait TableManager<K: LogSerial, V: LogSerial> {
    fn new(p: &Path) -> Self;
    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()>;
    fn read(&mut self, key: &K) -> Option<V>;
    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool;
}

