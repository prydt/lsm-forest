pub mod lsm_forest {
    use anyhow::Result;
    use bincode::{Decode, Encode};
    use std::{
        collections::BTreeMap,
        fs::{self, File},
        hash::Hash,
        path::Path, io::BufReader,
    };
    use std::io::Write;
    use crc32fast;

    trait LogSerial = Encode + Decode + Hash + Ord + 'static;

    struct Log {
        file: File,
    }

    #[derive(Encode, Decode, Debug)]
    struct LogEntry<K: LogSerial, V: LogSerial> {
        crc: u32,
        is_delete: bool,
        key: K,
        value: V,
    }

    impl<K: LogSerial,V: LogSerial> LogEntry<K, V> {
        fn check_crc(&self) -> bool {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(if self.is_delete {&[1]} else { &[0]});


            todo!()
        }
    }


    impl Log {
        fn new(path: &Path) -> Log {
            let file = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)
                .unwrap();

            Log { file }
        }

        fn append<K: LogSerial, V: LogSerial>(&mut self, entry: LogEntry<K, V>) -> Result<()> {
            let payload = bincode::encode_to_vec(entry, bincode::config::standard())?;
            self.file.write(&payload)?;
            self.file.flush()?;

            Ok(())
        }

        fn recovery<K: LogSerial, V: LogSerial>(&mut self) -> Result<BTreeMap<K, V>> {
            let mut reader = BufReader::new(&self.file);

            while let Ok(entry) = bincode::decode_from_reader::<LogEntry<K,V>, &mut BufReader<&File>, _>(&mut reader, bincode::config::standard()) {
                // TODO
            }

            todo!()
        }
    }

    struct LSMTree<K, V> {
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
}
