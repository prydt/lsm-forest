#![feature(trait_alias)]
mod log;
mod lsm_forest;
mod table_manager;
use crate::log::*;
use crate::lsm_forest::*;
use crate::table_manager::*;
use anyhow::Result;
use bincode::{Decode, Encode};
use crc32fast;
use std::io::Write;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::BufReader,
    path::Path,
};

use std::os::unix::fs::MetadataExt;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

trait LogSerial = Encode + Decode + Hash + Ord + 'static;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_log_new() {
        let p = Path::new("test/test_log_new.log");
        let log = Log::new(p);

        assert!(p.exists());
        // fs::remove_file(p).unwrap();
    }

    #[test]
    fn test_log_append_recovery() {
        let p = Path::new("test/test_log_append.log");
        let mut log = Log::new(p);

        // create btree with data
        // append to log
        // create new log and recover

        let mut memtable = BTreeMap::new();

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());

            let mut entry = LogEntry {
                crc: 0,
                // is_delete: false,
                key,
                value: value_opt,
            };

            entry.set_crc();

            log.append(entry);
        }

        let mut other_log = Log::new(p);
        let recovered_memtable: BTreeMap<String, Option<String>> = other_log.recovery().unwrap();

        for (k, v) in memtable.iter() {
            //assert_eq!(v, &recovered_memtable.get(k).as_mut().unwrap().as_mut().unwrap());
        }
    }

    #[test]
    fn test_simple_tm_new() {
        let p = Path::new("test/test_simple_tm_new");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut names = Vec::new();
        for i in 0..100 {
            sleep(Duration::from_millis(1));
            let name = format!(
                "sstable_{}.sst",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            );
            names.push(p.join(name.clone()).to_path_buf());
            File::create(p.join(name)).unwrap();
        }

        let tm: SimpleTableManager<String, String> = SimpleTableManager::new(p);

        for i in 0..names.len() {
            //assert!(tm.sstables.contains(&name));
            assert_eq!(tm.sstables[i], names[i]);
        }
        // assert_eq!(tm.sstables, names);
        // fs::remove_dir_all(p).unwrap();

        fs::remove_dir_all(p).unwrap();
    }

    #[test]
    fn test_simple_tm_add_table() {
        let p = Path::new("test/test_simple_tm_add_table");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut tm = SimpleTableManager::<String, String>::new(p);
        let mut memtable = BTreeMap::new();

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());
        }

        

        assert_eq!(tm.add_table(memtable.clone()).unwrap(), ());
        assert_eq!(tm.sstables.len(), 1);
        assert!(tm.sstables[0].exists());

        let f = File::open(tm.sstables[0].clone()).unwrap();
        let mut reader = BufReader::new(&f);
        while let Ok(entry) = bincode::decode_from_reader::<SimpleTableEntry<String,String>, &mut BufReader<&File>, _>(&mut reader, bincode::config::standard()) {
            assert_eq!(entry.value, memtable.get(&entry.key).unwrap().clone());
        }
    }

    #[test]
    fn test_simple_tm_should_flush() {
        let p = Path::new("test/test_simple_tm_should_flush");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut tm = SimpleTableManager::<String, String>::new(p);
        let mut memtable = BTreeMap::new();

        assert_eq!(tm.should_flush(LSMTree { wal: Log{ file: File::create(p.join("temp")).unwrap()}, memtable: memtable.clone() }), false);

        for i in 0..63 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());

            println!("{} {}" , i, memtable.len());
            assert_eq!(tm.should_flush(LSMTree { wal: Log{ file: File::create(p.join("temp")).unwrap()}, memtable: memtable.clone() }), false);
        }



        for i in 64..200 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());
        
            assert_eq!(tm.should_flush(LSMTree { wal: Log{ file: File::create(p.join("temp")).unwrap()}, memtable: memtable.clone() }), true);
        }

        
    }

    //      fillseq       -- write N values in sequential key order in async mode
    //      fillrandom    -- write N values in random key order in async mode
    //      overwrite     -- overwrite N values in random key order in async mode
    //      fillsync      -- write N/100 values in random key order in sync mode
    //      fill100K      -- write N/1000 100K values in random order in async mode
    //      deleteseq     -- delete N keys in sequential order
    //      deleterandom  -- delete N keys in random order
    //      readseq       -- read N times sequentially
    //      readreverse   -- read N times in reverse order
    //      readrandom    -- read N times in random order
    //      readmissing   -- read N missing keys in random order
    //      readhot       -- read N times in random order from 1% section of DB
    //      seekrandom    -- N random seeks
    //      seekordered   -- N ordered seeks
    //      open          -- cost of opening a DB
    //      crc32c        -- repeated crc32c of 4K of data
    //   Meta operations:
    //      compact     -- Compact the entire DB
    //      stats       -- Print DB stats
    //      sstables    -- Print sstable info
}
