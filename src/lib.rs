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

use csv::Writer;
use fs_extra::dir::get_size;
use rand::prelude::*;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use crate::table_manager::bcat_table_manager::*;
use crate::table_manager::simple_bloom_table_manager::*;
use crate::table_manager::simple_cache_table_manager::*;
use crate::table_manager::simple_compact_table_manager::*;
use crate::table_manager::simple_table_manager::*;
use crate::table_manager::tiered_compact_table_manager::*;

const TEST_N: i64 = 4096;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_new() {
        let p = Path::new("test/test_log_new.log");
        let _log = Log::new(p);

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

            let _ = log.append(entry);
        }

        let mut other_log = Log::new(p);
        let recovered_memtable: BTreeMap<String, Option<String>> = other_log.recovery().unwrap();

        for (k, v) in memtable.iter() {
            assert_eq!(v, recovered_memtable.get(k).unwrap());
        }
    }

    #[test]
    fn test_simple_tm_new() {
        let p = Path::new("test/test_simple_tm_new");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut names = Vec::new();
        for i in 0..TEST_N {
            sleep(Duration::from_millis(1));
            let name = format!("sstable_{:08}.sst", i);
            names.push(p.join(name.clone()).to_path_buf());
            File::create(p.join(name)).unwrap();
        }

        let tm: BCATTableManager<String, String> = BCATTableManager::new(p);

        for i in 0..names.len() {
            //assert!(tm.sstables.contains(&name));
            assert_eq!(tm.tm.tm.sstables[i], names[i]);
        }
        // assert_eq!(tm.sstables, names);
        // fs::remove_dir_all(p).unwrap();
    }

    #[test]
    fn test_tm_add_table() {
        let p = Path::new("test/test_simple_tm_add_table");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::<String, String>::new(p);
        let mut memtable = BTreeMap::new();

        for i in 0..TEST_N {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());
        }

        assert_eq!(tm.add_table(memtable.clone()).unwrap(), ());
        // assert_eq!(tm.sstables.len(), 1);
        // assert!(tm.sstables[0].exists());

        let f = File::open(tm.tm.tm.sstables[0].clone()).unwrap();
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

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let tm = BCATTableManager::<String, String>::new(p);
        let mut memtable = BTreeMap::new();

        let dummy_wal = Log::new(&p.join("temp"));

        assert_eq!(tm.should_flush(&dummy_wal, &memtable), false);

        for i in 0..255 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());

            println!("{} {}", i, memtable.len());
            assert_eq!(tm.should_flush(&dummy_wal, &memtable), false);
        }

        for i in 256..400 {
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

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::<String, String>::new(p);
        let mut memtable = BTreeMap::new();

        for i in 0..512 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let value_opt = Some(value.clone());
            memtable.insert(key.clone(), value_opt.clone());
        }

        let mut copy_memtable = memtable.clone();

        for i in 0..512 {
            let key = format!("key{}", i);
            let mut value = copy_memtable.get(&key).unwrap().as_ref().unwrap().clone();
            value = format!("{}actual", value);
            copy_memtable.insert(key.clone(), Some(value.clone()));
            memtable.insert(key.clone(), Some(value.clone()));
            sleep(Duration::from_millis(1));
            tm.add_table(copy_memtable.clone())
                .expect("add table failed");
            copy_memtable.remove(&key);
        }

        for i in 0..512 {
            let key = format!("key{}", i);
            assert_eq!(tm.read(&key), memtable.get(&key.clone()).unwrap().clone());
        }
    }

    #[test]
    fn test_lsm_tree_new() {
        // TODO
    }

    #[test]
    fn test_lsm_put_get() {
        let p = Path::new("test/test_lsm_put_get");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::<String, String>::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
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

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
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
        let p = Path::new("test/test_lsm_remove");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in 0..TEST_N {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            lsm.put(key.clone(), value.clone()).expect("put failed");
        }

        for i in 0..TEST_N {
            let key = format!("key{}", i);
            lsm.remove(&key).expect("remove failed");
            assert_eq!(lsm.get(&key), None);
        }
    }

    #[test]
    fn test_lsm_remove_random() {
        let p = Path::new("test/test_lsm_remove_random");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        let mut rng = rand::thread_rng();

        for i in 0..TEST_N {
            let key = i;
            let value = i;
            lsm.put(key.clone(), value.clone()).expect("put failed");
        }

        let mut keys: Vec<i64> = (0..TEST_N).collect();
        keys.shuffle(&mut rng);

        for key in keys {
            lsm.remove(&key).expect("remove failed");
            assert_eq!(lsm.get(&key), None);
        }

        // for i in 0..TEST_N {
        //     lsm.remove(&i).expect("remove failed");
        //     assert_eq!(lsm.get(&i), None);
        // }
    }

    #[test]
    fn test_lsm_flush() {
        let p = Path::new("test/test_lsm_flush");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        // add 64 entries to memtable
        // check if memtbale is cleared
        // check if wal is cleared
        // check if sstable is created

        for i in 0..255 {
            let key = i;
            let value = i;
            lsm.put(key, value).expect("put failed");
        }

        assert_ne!(lsm.memtable.read().unwrap().len(), 0);
        lsm.put(256, 256).expect("put failed");
        assert_eq!(lsm.memtable.read().unwrap().len(), 0);
        assert_eq!(lsm.wal.lock().unwrap().file.metadata().unwrap().len(), 0);
        assert!(lsm.table_manager.lock().unwrap().tm.tm.sstables[0].exists());

        for i in 0..255 {
            assert_eq!(lsm.table_manager.lock().unwrap().read(&i), Some(i));
            assert_eq!(lsm.get(&i), Some(i));
        }

        for i in 256..511 {
            let key = i;
            let value = i;
            lsm.put(key, value).expect("put failed");
        }

        assert_ne!(lsm.memtable.read().unwrap().len(), 0);
        lsm.put(255, 255).expect("put failed");
        assert_eq!(lsm.memtable.read().unwrap().len(), 0);
        assert_eq!(lsm.wal.lock().unwrap().file.metadata().unwrap().len(), 0);
        assert!(lsm.table_manager.lock().unwrap().tm.tm.sstables[1].exists());
        assert!(lsm.table_manager.lock().unwrap().tm.tm.sstables[0].exists());

        for i in 0..256 {
            assert_eq!(lsm.table_manager.lock().unwrap().read(&i), Some(i));
            assert_eq!(lsm.get(&i), Some(i));
        }
    }

    #[test]
    fn test_lsm_recovery() {
        let p = Path::new("test/test_lsm_recovery");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let mut tm = BCATTableManager::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in 0..63 {
            lsm.put(i, i).expect("put failed");
        }

        let mut tm2 = BCATTableManager::new(p);
        let lsm2 = LSMTree::new(p.to_path_buf(), &mut tm2);

        for i in 0..63 {
            assert_eq!(lsm2.get(&i), Some(i));
            assert_eq!(
                lsm.memtable.read().unwrap().get(&i),
                lsm2.memtable.read().unwrap().get(&i)
            );
        }
    }

    #[test]
    fn test_lsm_threads() {
        let p = Path::new("test/test_lsm_threads");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        let temp_box = Box::new(BCATTableManager::new(p));
        let tm = Box::leak(temp_box);
        let lsm = Arc::new(LSMTree::new(p.to_path_buf(), tm));
        let mut threads = Vec::new();

        for i in 1..=512 {
            let my_lsm = Arc::clone(&lsm);
            threads.push(std::thread::spawn(move || {
                for j in 0..64 {
                    let (key, value) = (i * 1048 + j, i * 1048 + j);
                    my_lsm.put(key, value).expect("put failed");
                    assert_eq!(my_lsm.get(&key).expect("get failed"), value);
                    my_lsm.remove(&key).expect("remove failed");
                    assert_eq!(my_lsm.get(&key), None);
                }
            }));
        }

        for j in threads {
            j.join().unwrap();
        }
    }

    fn fillseq<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in 0..n {
            let key = format!("{}", i);
            let value = format!("{}", i);
            lsm.put(key, value)?;
        }

        Ok(())
    }

    fn readseq<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in 0..n {
            let key = format!("{}", i);
            lsm.get(&key);
        }

        Ok(())
    }

    fn deleteseq<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in 0..n {
            let key = format!("{}", i);
            lsm.remove(&key).expect("remove failed");
        }

        Ok(())
    }

    fn fillrand<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
        let mut rng = rand::thread_rng();

        let mut keys: Vec<i64> = (0..n).collect();
        keys.shuffle(&mut rng);

        for key in keys {
            let key = format!("{}", key);
            let value = key.clone();
            lsm.put(key, value).expect("put failed");
        }

        Ok(())
    }

    fn readrand<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
        let mut rng = rand::thread_rng();

        let mut keys: Vec<i64> = (0..n).collect();
        keys.shuffle(&mut rng);

        for key in keys {
            let key = format!("{}", key);
            lsm.get(&key);
        }

        Ok(())
    }

    fn deleterand<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
        let mut rng = rand::thread_rng();

        let mut keys: Vec<i64> = (0..n).collect();
        keys.shuffle(&mut rng);

        for key in keys {
            let key = format!("{}", key);
            lsm.remove(&key).expect("remove failed");
        }

        Ok(())
    }

    fn _readreverse<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in (0..n).rev() {
            let key = format!("{}", i);
            let value = format!("{}", i);
            lsm.get(&key);
        }

        Ok(())
    }

    fn overwrite<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
        let mut rng = rand::thread_rng();

        let mut keys: Vec<i64> = (0..n).collect();
        keys.shuffle(&mut rng);

        for i in keys {
            let key = format!("{}", i);
            let value = format!("{}", i);
            lsm.put(key, value).expect("put failed");
        }

        Ok(())
    }

    fn readmissing<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);

        for i in n..n * 2 {
            let key = format!("{}", i);
            lsm.get(&key);
        }

        Ok(())
    }

    fn readhot<TM: TableManager<String, String>>(p: &Path, n: i64) -> Result<()> {
        let mut tm = TM::new(p);
        let lsm = LSMTree::new(p.to_path_buf(), &mut tm);
        let mut rng = rand::thread_rng();

        let mut keys: Vec<i64> = (0..n).collect();
        keys.shuffle(&mut rng);
        keys.truncate((n / 100) as usize);

        for _ in 0..n {
            let index = keys.get(rng.gen_range(0..(n / 100) as usize)).unwrap();
            let key = format!("{}", index);
            lsm.get(&key);
        }
        Ok(())
    }

    #[test]
    fn test_lsm_fillseq_readseq() {
        let n = 10_000;
        let p = Path::new("test/lsm_fillseq_readseq");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        readseq::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_fillseq_readseq");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        readseq::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    #[test]
    fn test_lsm_fillseq_deleteseq() {
        let n = 10_000;
        let p = Path::new("test/lsm_fillseq_deleteseq");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        deleteseq::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_fillseq_deleteseq");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        deleteseq::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    #[test]
    fn test_lsm_fillrand_readrand() {
        let n = 10_000;
        let p = Path::new("test/lsm_fillrand_readrand");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillrand::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        readrand::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_fillrand_readrand");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillrand::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        readrand::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    #[test]
    fn test_lsm_fillrand_deleterand() {
        let n = 10_000;
        let p = Path::new("test/lsm_fillrand_deleterand");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillrand::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        deleterand::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_fillrand_deleterand");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillrand::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        deleterand::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    #[test]
    fn test_lsm_overwrite() {
        let n = 10_000;
        let p = Path::new("test/lsm_overwrite");
        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        overwrite::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_overwrite");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        overwrite::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    #[test]
    fn test_lsm_readmissing() {
        let n = 10_000;
        let p = Path::new("test/lsm_readmissing");
        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        readmissing::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_readmissing");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        readmissing::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    #[test]
    fn test_lsm_readhot() {
        let n = 10_000;
        let p = Path::new("test/lsm_readhot");
        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleTableManager<String, String>>(p, n).expect("fillseq failed");
        readhot::<SimpleTableManager<String, String>>(p, n).expect("readseq failed");

        let p = Path::new("test/lsm_compact_readhot");

        let _ = fs::remove_dir_all(p);
        let _ = fs::create_dir(p);

        fillseq::<SimpleCompactTableManager<String, String>>(p, n).expect("fillseq failed");
        readhot::<SimpleCompactTableManager<String, String>>(p, n).expect("readseq failed");
    }

    fn benchmark<TM: TableManager<String, String>>(
        name: String,
        time_wtr: &mut Writer<File>,
        space_wtr: &mut Writer<File>,
    ) {
        let n = 10_000;
        let iterations = 5;
        let p = Path::new("test/benchmark");

        let benchmarks = [
            deleteseq::<TM>,
            deleterand::<TM>,
            readseq::<TM>,
            readrand::<TM>,
            readmissing::<TM>,
            readhot::<TM>,
            overwrite::<TM>,
        ];
        let benchmarks_fill = [fillseq::<TM>, fillrand::<TM>];
        let mut benchmark_time_results = vec![name.clone()];
        let mut benchmark_space_results = vec![name.clone()];

        for benchmark in benchmarks_fill {
            println!("{} {}", name.clone(), format!("{:?}", benchmark));
            let mut total_time = 0;
            let mut total_space = 0;
            for _ in 0..iterations {
                let _ = fs::remove_dir_all(p);
                let _ = fs::create_dir(p);

                let start = SystemTime::now();
                benchmark(p, n).expect("benchmark failed");
                let end = SystemTime::now();
                total_time += end.duration_since(start).unwrap().as_millis();
                total_space += get_size(p).expect("get_size failed");
            }
            let avg_time = total_time as f64 / iterations as f64;
            benchmark_time_results.push(format!("{}", avg_time));

            let avg_space = total_space as f64 / iterations as f64;
            benchmark_space_results.push(format!("{}", avg_space));
        }

        for benchmark in benchmarks {
            println!("{} {}", name.clone(), format!("{:?}", benchmark));
            let mut total_time = 0;
            let mut total_space = 0;
            for _ in 0..iterations {
                let _ = fs::remove_dir_all(p);
                let _ = fs::create_dir(p);
                fillseq::<TM>(p, n).expect("fillseq failed");

                let start = SystemTime::now();
                benchmark(p, n).expect("benchmark failed");
                let end = SystemTime::now();
                total_time += end.duration_since(start).unwrap().as_millis();
                total_space += get_size(p).expect("get_size failed");
            }
            let avg_time = total_time as f64 / iterations as f64;
            benchmark_time_results.push(format!("{}", avg_time));

            let avg_space = total_space as f64 / iterations as f64;
            benchmark_space_results.push(format!("{}", avg_space));
        }

        time_wtr
            .write_record(&benchmark_time_results)
            .expect("CSV write failed");
        time_wtr.flush().expect("CSV flush failed");

        space_wtr
            .write_record(&benchmark_space_results)
            .expect("CSV write failed");
        space_wtr.flush().expect("CSV flush failed");
    }

    #[test]
    fn run_benchmark() {
        let mut time_wtr = csv::Writer::from_path("test/benchmark_time.csv").unwrap();
        let mut space_wtr = csv::Writer::from_path("test/benchmark_space.csv").unwrap();

        let benchmark_header = [
            "tablemanager",
            "fillseq",
            "fillrand",
            "deleteseq",
            "deleterand",
            "readseq",
            "readrand",
            "readmissing",
            "readhot",
            "overwrite",
        ];
        time_wtr
            .write_record(benchmark_header)
            .expect("CSV write failed");
        time_wtr.flush().expect("CSV flush failed");
        space_wtr
            .write_record(benchmark_header)
            .expect("CSV write failed");
        space_wtr.flush().expect("CSV flush failed");

        benchmark::<SimpleTableManager<String, String>>(
            "simple".to_string(),
            &mut time_wtr,
            &mut space_wtr,
        );
        benchmark::<SimpleBloomTableManager<String, String>>(
            "bloom".to_string(),
            &mut time_wtr,
            &mut space_wtr,
        );
        benchmark::<SimpleCacheTableManager<String, String>>(
            "cache".to_string(),
            &mut time_wtr,
            &mut space_wtr,
        );
        benchmark::<SimpleCompactTableManager<String, String>>(
            "compact".to_string(),
            &mut time_wtr,
            &mut space_wtr,
        );
        benchmark::<TieredCompactTableManager<String, String>>(
            "tiered".to_string(),
            &mut time_wtr,
            &mut space_wtr,
        );
        benchmark::<BCATTableManager<String, String>>(
            "bcat".to_string(),
            &mut time_wtr,
            &mut space_wtr,
        );
    }

    fn multithread_benchmark<TM: TableManager<String, String>>(
        num_threads: i64,
        time_wtr: &mut Writer<File>,
    ) {
        let n = 10_000;
        let iterations = 5;
        let p = Path::new("test/benchmark_multithread");

        let mut benchmark_time_results = vec![format!("{}", num_threads)];
        let mut total_put_time = 0;
        let mut total_get_time = 0;

        for _ in 0..iterations {
            let _ = fs::remove_dir_all(p);
            let _ = fs::create_dir(p);

            let temp_box = Box::new(BCATTableManager::new(p));
            let tm = Box::leak(temp_box);
            // let mut tm = TM::<String,String>::new(p);
            // let mut tm = BCATTableManager::<String, String>::new(p);
            let lsm = Arc::new(LSMTree::new(p.to_path_buf(), tm));
            // let mut lsm_rc = Arc::new(lsm);
            let mut threads = Vec::new();
            let start = SystemTime::now();
            for i in 0..num_threads {
                let my_lsm = Arc::clone(&lsm);
                threads.push(std::thread::spawn(move || {
                    for j in 0..(n / num_threads) {
                        let key = format!("{}", i * (n / num_threads) + j);
                        let value = format!("{}", i * (n / num_threads) + j);
                        my_lsm.put(key, value).expect("put failed");
                    }
                }))
            }
            for thread in threads {
                thread.join().unwrap();
            }

            let end = SystemTime::now();
            total_put_time += end.duration_since(start).unwrap().as_millis();

            let mut threads = Vec::new();

            let start = SystemTime::now();
            for i in 0..num_threads {
                let my_lsm = Arc::clone(&lsm);
                threads.push(std::thread::spawn(move || {
                    for j in 0..(n / num_threads) {
                        let key = format!("{}", i * (n / num_threads) + j);
                        my_lsm.get(&key).expect("get failed");
                    }
                }))
            }
            for thread in threads {
                thread.join().unwrap();
            }
            let end = SystemTime::now();
            total_get_time += end.duration_since(start).unwrap().as_millis();
        }

        benchmark_time_results.push(format!("{}", total_put_time as f64 / iterations as f64));
        benchmark_time_results.push(format!("{}", total_get_time as f64 / iterations as f64));

        time_wtr
            .write_record(&benchmark_time_results)
            .expect("CSV write failed");
        time_wtr.flush().expect("CSV flush failed");
    }

    #[test]
    fn run_multithread_benchmark() {
        let mut time_wtr = csv::Writer::from_path("test/benchmark_multithread_time.csv").unwrap();

        let benchmark_header = ["num of threads", "write", "read"];
        time_wtr
            .write_record(benchmark_header)
            .expect("CSV write failed");
        time_wtr.flush().expect("CSV flush failed");

        for i in [1, 2, 4, 8, 16, 32] {
            multithread_benchmark::<BCATTableManager<String, String>>(i, &mut time_wtr);
        }
    }
}
