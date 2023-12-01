#![feature(trait_alias)]
#![allow(unused_imports)]
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
use rand::prelude::*;

trait LogSerial = Encode + Decode + Hash + Ord + 'static;
const TEST_N: i64 = 4096;

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

        for i in 0..TEST_N {
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
        for i in 0..TEST_N {
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

        for i in 0..TEST_N {
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
        while let Ok(entry) = bincode::decode_from_reader::<
            SimpleTableEntry<String, String>,
            &mut BufReader<&File>,
            _,
        >(&mut reader, bincode::config::standard())
        {
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

        let mut dummy_wal = Log::new(&p.join("temp"));

        assert_eq!(tm.should_flush(&dummy_wal, &memtable), false);

        for i in 0..63 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());

            println!("{} {}", i, memtable.len());
            assert_eq!(tm.should_flush(&dummy_wal, &memtable), false);
        }

        for i in 64..200 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());

            assert_eq!(tm.should_flush(&dummy_wal, &memtable), true);
        }
    }

    #[test]
    fn test_tm_read() {
        let p = Path::new("test/test_tm_read");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut tm = SimpleTableManager::<String, String>::new(p);
        let mut memtable = BTreeMap::new();

        for i in 0..256 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());
        }

        let mut copy_memtable = memtable.clone();

        for i in 0..256 {
            let key = format!("key{}", i);
            let mut value = copy_memtable.get(&key).unwrap().as_ref().unwrap().clone();
            value = format!("{}actual", value);
            copy_memtable.insert(key.clone(), Some(value.clone()));
            memtable.insert(key.clone(), Some(value.clone()));
            sleep(Duration::from_millis(1));
            tm.add_table(copy_memtable.clone()).expect("add table failed");
            copy_memtable.remove(&key);
        }

        for i in 0..256 {
            let key = format!("key{}", i);
            assert_eq!(
                tm.read(&key),
                memtable.get(&key.clone()).unwrap().clone()
            );
        }
    }

    #[test]
    fn test_lsm_tree_new() {
        // TODO
    }

    #[test]
    fn test_lsm_put_get() {
        let p = Path::new("test/test_lsm_put_get");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut tm = SimpleTableManager::<String, String>::new(p);
        let mut lsm =
            LSMTree::<String, String>::new(p.to_path_buf(), &mut tm);
        let mut memtable = BTreeMap::new();

        for i in 0..TEST_N {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            memtable.insert(key.clone(), value.clone());
            lsm.put(key.clone(), value.clone()).expect("put failed");
            assert_eq!(lsm.get(&key), Some(value));
        }

        for (k, v) in memtable.iter() {
            assert_eq!(lsm.get(&k), Some(v.clone()));
        }
    }

    #[test]
    fn test_lsm_put_get_random() {
        let p = Path::new("test/test_lsm_put_get_random");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut tm = SimpleTableManager::new(p);
        let mut lsm =
            LSMTree::new(p.to_path_buf(), &mut tm);
        let mut memtable = BTreeMap::new();
        let mut rng = rand::thread_rng();
        

        for _i in 0..TEST_N {
            let key: String = format!("{}", rng.gen::<i32>());
            let value: String = format!("{}", rng.gen::<i32>());
            memtable.insert(key.clone(), value.clone());
            lsm.put(key.clone(), value.clone()).expect("put failed");
            assert_eq!(lsm.get(&key), Some(value));
        }

        for (k, v) in memtable.iter() {
            // assert_eq!(lsm.get(&k), v.clone());
            assert_eq!(lsm.get(&k), Some(v.clone()));
        }
    }

    #[test]
    fn test_lsm_remove() {
        // let p = Path::new("test/test_lsm_remove");

        // fs::remove_dir_all(p);
        // fs::create_dir(p);

        // let mut tm = SimpleTableManager::<String, String>::new(p);
        // let mut lsm =
        //     LSMTree::<String, String>::new(p.to_path_buf(), &mut tm);

        // for i in 0..TEST_N {
        //     let key = format!("key{}", i);
        //     let value = format!("value{}", i);
        //     lsm.put(key.clone(), value.clone()).expect("put failed");
        //     assert_eq!(lsm.get(&key), Some(value));
        // }
    }

    #[test]
    fn test_lsm_flush() {
        let p = Path::new("test/test_lsm_flush");

        fs::remove_dir_all(p);
        fs::create_dir(p);

        let mut tm = SimpleTableManager::new(p);
        let mut lsm =
            LSMTree::new(p.to_path_buf(), &mut tm);

        // add 64 entries to memtable
        // check if memtbale is cleared
        // check if wal is cleared
        // check if sstable is created

        for i in 0..63 {
            let key = i;
            let value = i;
            lsm.put(key, value).expect("put failed");
        }

        assert_ne!(lsm.memtable.len(), 0);
        lsm.put(63, 63).expect("put failed");
        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.wal.file.metadata().unwrap().len(), 0);
        assert!(lsm.table_manager.sstables[0].exists());

        for i in 0..64 {
            assert_eq!(lsm.table_manager.read(&i), Some(i));
            assert_eq!(lsm.get(&i), Some(i));
        }

        for i in 64..127 {
            let key = i;
            let value = i;
            lsm.put(key, value).expect("put failed");
        }

        assert_ne!(lsm.memtable.len(), 0);
        lsm.put(127, 127).expect("put failed");
        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.wal.file.metadata().unwrap().len(), 0);
        assert!(lsm.table_manager.sstables[1].exists());
        assert!(lsm.table_manager.sstables[0].exists());

        for i in 0..128 {
            assert_eq!(lsm.table_manager.read(&i), Some(i));
            assert_eq!(lsm.get(&i), Some(i));
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
