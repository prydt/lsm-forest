# lsm-forest
- [x] synchronization
    - ~~create lock per ss_table (make sure threads read files in order)~~
    - ~~for compaction: thread per file, each reads file and contructs btree, return btree and have one thread merge in correct order and write~~
    - rwlock for memtable
    - one giant lock for sstables
    - one giant lock for log
- [ ] bloom filter
- [ ] compaction
    - simple: many files into 1
    - advanced: second level compacts files from first level after threshold. third level compacts files from second and third level after threshold.
- [x] log recovery
    - add log.delete
    - ~~move log.recovery to log.new~~
- [x] lsm tree recovery
- [ ] read cache in table_manager
- [ ] add multithreading to sstable read
- [ ] fix should_flush, account for wal length



tests:
- [ ] synchronization tests
- [ ] recovery tests


benchmarks:
- [ ] fill_seq
- [ ] fill_rand

- [ ] delete_seq
- [ ] delete_rand

- [ ] read_seq
- [ ] read_rand

- [ ] overwrite (rand)
- [ ] read_missing (rand)
- [ ] read_hot (rand from 1% of DB)
- [ ] compact








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