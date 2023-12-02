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

impl<K: LogSerial, V: LogSerial> TableManager<K, V> for SimpleTableManager<K, V> {
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

        SimpleTableManager::<K, V> {
            sstables,
            path: p.to_path_buf(),
            phantom: std::marker::PhantomData,
        }
    }

    fn add_table(&mut self, memtable: BTreeMap<K, Option<V>>) -> Result<()> {
        let name = format!("sstable_{:08}.sst", self.sstables.len());
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
            let entry = SimpleTableEntry { key, value };
            let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
            file.write(&payload)?;
        }
        file.flush()?;
        Ok(())
    }

    fn read(&mut self, key: &K) -> Option<V> {
        self.sstables.sort();
        for path in self.sstables.iter().rev() {
            let f = File::open(path).unwrap();
            let mut reader = std::io::BufReader::new(&f);
            while let Ok(entry) = bincode::decode_from_reader::<
                SimpleTableEntry<K, V>,
                &mut std::io::BufReader<&File>,
                _,
            >(&mut reader, bincode::config::standard())
            {
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
        memtable.len() >= 256 || wal.file.metadata().unwrap().len() >= (4 * 1024)
    }
}
