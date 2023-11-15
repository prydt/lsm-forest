pub mod lsm_forest {
    use std::{collections::BTreeMap, fs::File};

    struct LogFile<K, V> {
        file: File,
    }

    struct LSMTree<K, V> {
        memtable: BTreeMap<K, V>,
    }
}
