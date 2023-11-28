#![feature(trait_alias)]
mod lsm_forest;
use crate::lsm_forest::*;
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

trait LogSerial = Encode + Decode + Hash + Ord + 'static;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_log_new() {
        let p = Path::new("test_log_new.log");
        let log = Log::new(p);

        assert!(p.exists());
        fs::remove_file(p).unwrap();
    }

    #[test]
    fn test_log_append() {
        let p = Path::new("test_log_append.log");
        let mut log = Log::new(p);

        // create btree with data
        // append to log
        // create new log and recover

        let mut memtable = BTreeMap::new();

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = (format!("value{}", i));
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());

            let mut entry = LogEntry {
                crc: 0,
                // is_delete: false,
                key,
                value:value_opt,
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
