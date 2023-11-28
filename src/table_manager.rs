use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::BTreeMap;

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

        // let name = format!("sstable_{}.sst", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis());
        // let mut file = File::create(self.path.join(name))?;

        todo!()
        
    }
    // fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
    //     let mut path = Path::new("data/");
    //     let mut file = File::create(path.join("test.sst"))?;
    //     let mut buf = Vec::new();
    //     for (key, value) in memtable {
    //         let entry = LogEntry {
    //             crc: 0,
    //             key,
    //             value,
    //         };
    //         entry.set_crc();
    //         buf.write(&entry.encode())?;
    //     }
    //     file.write(&buf)?;
    //     self.sstables.push(file);
    //     Ok(())
    // }
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