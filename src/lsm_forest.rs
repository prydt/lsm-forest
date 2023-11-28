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

    pub trait LogSerial = Encode + Decode + Hash + Ord + 'static + Debug;

    pub struct Log {
        file: File,
    }

    #[derive(Encode, Decode, Debug)]
    pub struct LogEntry<K: LogSerial , V: LogSerial> {
        pub crc: u32,
        // pub is_delete: bool,
        pub key: K,
        pub value: V,
    }

    impl<K: LogSerial,V: LogSerial> LogEntry<K, Option<V>> {
        pub fn compute_crc(&self) -> u32 {
            let mut hasher = crc32fast::Hasher::new();
            let mut h = DefaultHasher::new();
            // self.is_delete.hash(&mut h);
            self.key.hash(&mut h);
            self.value.hash(&mut h);
            hasher.update(&h.finish().to_le_bytes());

            hasher.finalize()
        }

        pub fn check_crc(&self) -> bool {
            self.crc == self.compute_crc()
        }

        pub fn set_crc(&mut self) {
            self.crc = self.compute_crc();
        }
    }


    impl Log {
        pub fn new(path: &Path) -> Log {
            let file = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)
                .unwrap();

            Log { file }
        }

        pub fn append<K: LogSerial, V: LogSerial>(&mut self, entry: LogEntry<K, Option<V>>) -> Result<()> {
            let payload = bincode::encode_to_vec(&entry, bincode::config::standard())?;
            self.file.write(&payload)?;
            self.file.flush()?;

            Ok(())
        }

        pub fn recovery<K: LogSerial, V: LogSerial>(&mut self) -> Result<BTreeMap<K, Option<V>>> {
            let mut reader = BufReader::new(&self.file);

            let mut memtable = BTreeMap::new();

            println!("start recovery\n");
            while let Ok(entry) = bincode::decode_from_reader::<LogEntry<K,Option<V>>, &mut BufReader<&File>, _>(&mut reader, bincode::config::standard()) {
                println!("entry: {:?}\n", entry);
                println!("entry v: {:?}\n", entry.value);

                if entry.check_crc() {
                    // if entry.is_delete {
                        // memtable.insert(entry.key, None);
                    // } else {
                        // memtable.insert(entry.key, Some(entry.value));
                    // }
                    memtable.insert(entry.key, entry.value);
                }
            }

            Ok(memtable)
        }
    }

    pub struct LSMTree<K, V> {
        wal: Log,
        memtable: BTreeMap<K, Option<V>>,
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

    trait TableManager<K: LogSerial, V: LogSerial> {
        fn new() -> Self;
        fn add_table(lsm: LSMTree<K, V>);
        fn read(key: K) -> Option<V>;
        fn should_flush(lsm: LSMTree<K, V>) -> bool;
    }

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
