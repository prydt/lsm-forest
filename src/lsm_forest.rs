use anyhow::Result;
use bincode::{Decode, Encode};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    hash::{Hash, DefaultHasher, Hasher},
    path::Path, io::BufReader,
};
use std::io::Write;
use crc32fast;
use core::fmt::Debug;
use crate::log::*;

pub trait LogSerial = Encode + Decode + Hash + Ord + 'static + Debug;




pub struct LSMTree<K, V> {
    pub wal: Log,
    pub memtable: BTreeMap<K, Option<V>>,
}

/// LSM Tree
impl<K: LogSerial, V: LogSerial> LSMTree<K, V> {
    /// Create a new LSM Tree
    fn new() -> LSMTree<K, V> {
        todo!()
    }

    fn get(&self, key: K) -> Option<V> {
        todo!()
    }

    fn put(&mut self, key: K, value: V) {
        todo!()
    }

    fn remove(&mut self, key: K) {
        todo!()
    }
}


