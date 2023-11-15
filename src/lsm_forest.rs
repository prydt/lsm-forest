use bincode::{Decode, Encode};

pub mod lsm_forest {
    use std::{collections::BTreeMap, fs::File};

    struct LogFile<K, V> {
        file: File,
    }

    #[derive(Encode, Decode, Ord, Debug)]
    struct LogEntry<
    K: Encode + Decode + Hash + Ord + 'static,
    V: Encode + Decode + Hash + Ord + 'static,

    >{
    }

    struct LSMTree<K, V> {
        memtable: BTreeMap<K, V>,
    }

    impl<K, V> LSMTree<K, V> {
        fn new() -> Self {
            LSMTree {
                memtable: BTreeMap::new(),
            }
        }
    }

    trait TableManager<K, V> {
        fn get(&self, key: K) -> Option<V>;
        fn put(&mut self, key: K, value: V);
        fn delete(&mut self, key: K);
    }
}
