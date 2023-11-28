use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use bincode::{Encode, Decode};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::BTreeMap;
use std::time::SystemTime;
use std::io::Write;

pub trait TableManager<K: LogSerial, V: LogSerial> {
    fn new(p: &Path) -> Self;
    fn add_table(memtable: BTreeMap<K, Option<V>>) -> Result<()>;
    fn read(key: K) -> Option<V>;
    fn should_flush(lsm: LSMTree<K, V>) -> bool;
}

pub struct SimpleTableManager<K: LogSerial, V: LogSerial> {
    // store sstable names with unix timestamp in an array
    pub sstables: Vec<PathBuf>,
    pub path: PathBuf,
    phantom: std::marker::PhantomData<(K, V)>,
}

#[derive(Encode, Decode, Debug)]

pub struct SimpleTableEntry<K: LogSerial, V: LogSerial> {
    pub key: K,
    pub value: Option<V>,
}


impl<K: LogSerial, V: LogSerial> SimpleTableManager<K, V> {
    pub fn new (p: &Path) -> Self {
        let mut sstables = Vec::new();
        
        for file in fs::read_dir(p).unwrap() {
            let file = file.unwrap();
            let path = file.path();
            if (path.extension().unwrap() == "sst") {
                sstables.push(path);
            }
        }

        sstables.sort();

        SimpleTableManager::<K, V> {
            sstables,
            path: p.to_path_buf(),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {

        let name = format!("sstable_{}.sst", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis());
        let path = self.path.join(&name);
        self.sstables.push(path.clone());
        //let mut file = File::create(self.path.join(name))?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        // let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
        // self.file.write(&payload)?;
        // // self.file.flush()?;
        for (key, value) in memtable {
            let entry = SimpleTableEntry {
                key,
                value,
            };
            let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
            file.write(&payload)?;   
        }
        file.flush()?;
        Ok(())
    }

    pub fn should_flush(&self, lsm: LSMTree<K, V>) -> bool {
        lsm.memtable.len() >= 64
    }

    
}
// new: take in path to db, add files to vector


// impl<K, V> LSMTree<K, V> {
//     fn new() -> Self {
//         LSMTree {
//             memtable: BTreeMap::new(),
//         }
//     }
// }

// trait TableManager<K, V> {
//     fn get(&self, key: K) -> Option<V>;
//     fn put(&mut self, key: K, value: V);
//     fn delete(&mut self, key: K);
// }