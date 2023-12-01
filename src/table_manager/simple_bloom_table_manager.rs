use crate::lsm_forest::LogSerial;
use crate::{log::*, lsm_forest::LSMTree};
use anyhow::Result;
use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::table_manager::TableManager;
use crate::table_manager::simple_table_manager::SimpleTableManager;

use bloomfilter::Bloom;

pub struct SimpleBloomTableManager<K: LogSerial, V: LogSerial> {
    pub tm: SimpleTableManager<K, V>,
    pub bloom: Bloom<K>,
}

// #[derive(Encode, Decode, Debug)]
// pub struct SimpleBloomTableEntry<K: LogSerial, V: LogSerial> {
//     pub key: K,
//     pub value: Option<V>,
// }

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for SimpleBloomTableManager<K, V> {
    fn new(p: &Path, estimate_max_count: usize, fp_rate: f64) -> Self {
        // let mut sstables = Vec::new();

        // for file in fs::read_dir(p).unwrap() {
        //     let file = file.unwrap();
        //     let path = file.path();

        //     match path.extension() {
        //         Some(ext) => {
        //             if ext == "sst" {
        //                 sstables.push(path);
        //             }
        //         }
        //         None => {}
        //     }
        // }

        // sstables.sort();

        SimpleBloomTableManager::<K, V>{
            tm: SimpleTableManager::<K, V>::new(p),
            bloom: Bloom::new_for_fp_rate(estimate_max_count, fp_rate),
        
        }
    }

    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
        // let name = format!("sstable_{:08}.sst", self.sstables.len());
        // let path = self.path.join(&name);
        // self.sstables.push(path.clone());
        // //let mut file = File::create(self.path.join(name))?;
        // let mut file = fs::OpenOptions::new()
        //     .create(true)
        //     .read(true)
        //     .write(true)
        //     .open(path)?;

        // // let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
        // // self.file.write(&payload)?;
        // // // self.file.flush()?;
        // for (key, value) in memtable {
        //     let entry = SimpleBloomTableEntry { key, value };
        //     let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
        //     file.write(&payload)?;
        // }
        // file.flush()?;
        Ok(())
    }

    fn read(&self, key: &K) -> Option<V> {
        // let mut reversed_sstables = self.sstables.clone();
        // reversed_sstables.rev();
        // println!("searching for key {:?}", key);
        // println!("sstables: {:?}", self.sstables);
        for path in self.sstables.iter().rev() {
            let f = File::open(path).unwrap();
            // println!("read from {:?}", path);
            let mut reader = std::io::BufReader::new(&f);
            while let Ok(entry) = bincode::decode_from_reader::<
                SimpleBloomTableEntry<K, V>,
                &mut std::io::BufReader<&File>,
                _,
            >(&mut reader, bincode::config::standard())
            {
                // println!("read entry {:?}", entry);
                if entry.key == key.clone() {
                    return entry.value;
                } else if entry.key > key.clone() {
                    break;
                }
            }
        }
        None
    }

    fn should_flush(&self, wal: &Log, memtable: &BTreeMap<K, Option<V>>) -> bool {
        // TODO check if wal is too big

        memtable.len() >= 64 // || wal.file.metadata().unwrap().len() >= (512 * 1024) S
    }
}
