//! MemTable — in-memory sorted buffer before SSTable flush.
//!
//! Incoming writes land here first (after the WAL). The BTreeMap keeps keys in
//! sorted order so an SSTable can be written in a single sequential pass.
//! When the estimated byte size crosses the configured threshold the memtable
//! signals that it should be flushed to disk.

use std::collections::BTreeMap;

/// In-memory sorted key-value buffer backed by a `BTreeMap`.
///
/// A `None` value represents a tombstone (deletion marker). The memtable
/// tracks its approximate byte size so the storage engine knows when to
/// freeze it and flush to an SSTable.
pub struct MemTable {
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Approximate byte footprint of all keys + values currently stored.
    size_bytes: usize,
    /// Flush threshold in bytes.
    max_bytes: usize,
}

impl MemTable {
    /// Create a new, empty memtable that will signal flush after `max_bytes`.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            size_bytes: 0,
            max_bytes,
        }
    }

    /// Insert or update a key-value pair.
    ///
    /// If the key already exists the old value is replaced and the size
    /// accounting is adjusted accordingly.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let new_value_len = value.len();
        let key_len = key.len();

        match self.entries.insert(key, Some(value)) {
            Some(old_value) => {
                // Key already existed — key cost is unchanged, swap value cost.
                let old_value_len = old_value.as_ref().map_or(0, Vec::len);
                self.size_bytes -= old_value_len;
                self.size_bytes += new_value_len;
            }
            None => {
                // Brand-new key — charge key + value.
                self.size_bytes += key_len + new_value_len;
            }
        }
    }

    /// Look up a key. Returns `Some(Some(value))` if the key is live,
    /// `Some(None)` if it's a tombstone, or `None` if absent.
    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.entries.get(key).map(|v| v.as_deref())
    }

    /// Record a deletion. Inserts a tombstone so that flushes propagate
    /// the delete to SSTables.
    pub fn delete(&mut self, key: Vec<u8>) {
        let key_len = key.len();
        match self.entries.insert(key, None) {
            Some(old_value) => {
                // Was already present — remove old value cost, key cost stays.
                self.size_bytes -= old_value.as_ref().map_or(0, Vec::len);
            }
            None => {
                // New tombstone entry — charge key cost only.
                self.size_bytes += key_len;
            }
        }
    }

    /// Returns `true` when the estimated size meets or exceeds the threshold.
    pub fn should_flush(&self) -> bool {
        self.size_bytes >= self.max_bytes
    }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the memtable contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Current approximate byte size.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

}

impl IntoIterator for MemTable {
    type Item = (Vec<u8>, Option<Vec<u8>>);
    type IntoIter = std::collections::btree_map::IntoIter<Vec<u8>, Option<Vec<u8>>>;

    /// Consume the memtable, yielding entries in sorted key order.
    ///
    /// Values are `Option<Vec<u8>>` — `None` means tombstone.
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_memtable_is_empty() {
        let mt = MemTable::new(1024);
        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);
        assert_eq!(mt.size_bytes(), 0);
        assert!(!mt.should_flush());
    }

    #[test]
    fn insert_and_get() {
        let mut mt = MemTable::new(1024);
        mt.insert(b"key1".to_vec(), b"value1".to_vec());

        assert_eq!(mt.get(b"key1"), Some(Some(b"value1".as_slice())));
        assert_eq!(mt.get(b"missing"), None);
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn insert_overwrites_value() {
        let mut mt = MemTable::new(1024);
        mt.insert(b"key".to_vec(), b"old".to_vec());
        mt.insert(b"key".to_vec(), b"new".to_vec());

        assert_eq!(mt.get(b"key"), Some(Some(b"new".as_slice())));
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn size_tracking_on_insert() {
        let mut mt = MemTable::new(1024);

        mt.insert(b"aaa".to_vec(), b"bbb".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 3);

        mt.insert(b"cc".to_vec(), b"dddd".to_vec());
        assert_eq!(mt.size_bytes(), 6 + 2 + 4);
    }

    #[test]
    fn size_tracking_on_overwrite() {
        let mut mt = MemTable::new(1024);

        mt.insert(b"key".to_vec(), b"short".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 5);

        // Overwrite with longer value.
        mt.insert(b"key".to_vec(), b"much_longer_value".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 17);

        // Overwrite with shorter value.
        mt.insert(b"key".to_vec(), b"x".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 1);
    }

    #[test]
    fn delete_inserts_tombstone() {
        let mut mt = MemTable::new(1024);
        mt.insert(b"key".to_vec(), b"value".to_vec());
        mt.delete(b"key".to_vec());

        // Tombstone is present.
        assert_eq!(mt.get(b"key"), Some(None));
        assert_eq!(mt.len(), 1);
        // Value cost removed, key cost remains.
        assert_eq!(mt.size_bytes(), 3);
    }

    #[test]
    fn delete_new_key_inserts_tombstone() {
        let mut mt = MemTable::new(1024);
        mt.delete(b"ghost".to_vec());

        assert_eq!(mt.get(b"ghost"), Some(None));
        assert_eq!(mt.len(), 1);
        assert_eq!(mt.size_bytes(), 5); // "ghost".len()
    }

    #[test]
    fn should_flush_triggers_at_threshold() {
        let mut mt = MemTable::new(10);
        assert!(!mt.should_flush());

        // 3 + 3 = 6, under threshold.
        mt.insert(b"aaa".to_vec(), b"bbb".to_vec());
        assert!(!mt.should_flush());

        // 6 + 2 + 4 = 12, over threshold of 10.
        mt.insert(b"cc".to_vec(), b"dddd".to_vec());
        assert!(mt.should_flush());
    }

    #[test]
    fn should_flush_exact_threshold() {
        let mut mt = MemTable::new(6);
        mt.insert(b"abc".to_vec(), b"def".to_vec());
        assert!(mt.should_flush()); // 6 == 6
    }

    #[test]
    fn into_iter_yields_sorted_order() {
        let mut mt = MemTable::new(1024);
        mt.insert(b"cherry".to_vec(), b"3".to_vec());
        mt.insert(b"apple".to_vec(), b"1".to_vec());
        mt.insert(b"banana".to_vec(), b"2".to_vec());

        let pairs: Vec<(Vec<u8>, Option<Vec<u8>>)> = mt.into_iter().collect();
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"apple".as_slice(), b"banana", b"cherry"]);
    }

    #[test]
    fn into_iter_includes_tombstones() {
        let mut mt = MemTable::new(1024);
        mt.insert(b"alive".to_vec(), b"yes".to_vec());
        mt.delete(b"dead".to_vec());

        let pairs: Vec<(Vec<u8>, Option<Vec<u8>>)> = mt.into_iter().collect();
        assert_eq!(pairs.len(), 2);

        assert_eq!(pairs[0].0, b"alive");
        assert_eq!(pairs[0].1, Some(b"yes".to_vec()));

        assert_eq!(pairs[1].0, b"dead");
        assert_eq!(pairs[1].1, None);
    }

    #[test]
    fn overwrite_tombstone_with_value() {
        let mut mt = MemTable::new(1024);
        mt.delete(b"key".to_vec());
        assert_eq!(mt.size_bytes(), 3);

        mt.insert(b"key".to_vec(), b"revived".to_vec());
        assert_eq!(mt.get(b"key"), Some(Some(b"revived".as_slice())));
        // key(3) + value(7) = 10
        assert_eq!(mt.size_bytes(), 3 + 7);
    }

    #[test]
    fn many_inserts_size_tracking() {
        let mut mt = MemTable::new(usize::MAX);
        let mut expected_size = 0usize;

        for i in 0u32..200 {
            let key = format!("k{i:04}").into_bytes();
            let val = format!("v{i:04}").into_bytes();
            expected_size += key.len() + val.len();
            mt.insert(key, val);
        }

        assert_eq!(mt.len(), 200);
        assert_eq!(mt.size_bytes(), expected_size);
    }
}
