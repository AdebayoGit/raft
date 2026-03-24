use std::fs;
use std::path::{Path, PathBuf};

use super::bloom::BloomFilter;
use super::error::SSTableError;
use super::SSTABLE_MAGIC;

/// A key-value pair where `None` value represents a tombstone.
type KvPair = (Vec<u8>, Option<Vec<u8>>);

/// Footer size in bytes.
const FOOTER_SIZE: usize = 32;

/// Reads an immutable SSTable file.
///
/// On `open`, the footer, index block, and bloom filter are loaded into
/// memory. Data blocks are read on demand during `get` and `scan`.
pub struct SSTableReader {
    path: PathBuf,
    /// Raw file contents (memory-mapped would be better for production,
    /// but a simple read-to-memory is correct and sufficient for Phase 1).
    data: Vec<u8>,
    bloom: BloomFilter,
    index: Vec<IndexEntry>,
    entry_count: u64,
}

/// Decoded index entry: first key of a data block and where to find it.
#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    length: u32,
}

impl SSTableReader {
    /// Open an SSTable file, validating the footer and loading the index
    /// and bloom filter into memory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SSTableError> {
        let path = path.as_ref().to_path_buf();
        let data = fs::read(&path)?;

        if data.len() < FOOTER_SIZE {
            return Err(SSTableError::BadMagic);
        }

        // ── Parse footer ──
        let footer_start = data.len() - FOOTER_SIZE;
        let footer = &data[footer_start..];

        let magic = &footer[28..32];
        if magic != SSTABLE_MAGIC {
            return Err(SSTableError::BadMagic);
        }

        let bloom_offset = u64::from_be_bytes(footer[0..8].try_into().unwrap()) as usize;
        let index_offset = u64::from_be_bytes(footer[8..16].try_into().unwrap()) as usize;
        let entry_count = u64::from_be_bytes(footer[16..24].try_into().unwrap());

        // ── Load bloom filter ──
        if bloom_offset > index_offset || index_offset > footer_start {
            return Err(SSTableError::CorruptIndex(
                "offsets out of range".to_string(),
            ));
        }
        let bloom_data = &data[bloom_offset..index_offset];
        let bloom = BloomFilter::decode(bloom_data).ok_or_else(|| {
            SSTableError::CorruptIndex("failed to decode bloom filter".to_string())
        })?;

        // ── Load index block ──
        let index_data = &data[index_offset..footer_start];
        let index = Self::decode_index(index_data)?;

        Ok(Self {
            path,
            data,
            bloom,
            index,
            entry_count,
        })
    }

    /// The file path this SSTable was loaded from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of key-value entries in the SSTable.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Read every entry in sorted order. Used by compaction to merge tables.
    pub fn scan_all(&self) -> Result<Vec<KvPair>, SSTableError> {
        let mut all = Vec::new();
        for ie in &self.index {
            all.extend(self.read_block(ie)?);
        }
        Ok(all)
    }

    /// Point lookup — returns `Some(Some(value))` for a live key,
    /// `Some(None)` for a tombstone, or `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, SSTableError> {
        // Fast path: bloom filter rejects definitely-absent keys.
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Find the candidate block via binary search on the index.
        let block_idx = match self.index.binary_search_by(|e| e.first_key.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(0) => return Ok(None), // key is before the first block
            Err(i) => i - 1,
        };

        let ie = &self.index[block_idx];
        self.search_block(ie, key)
    }

    /// Range scan — returns all entries with `start <= key < end` in sorted
    /// order. Both bounds are byte-slice keys. If `end` is `None`, scans to
    /// the end of the table.
    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<KvPair>, SSTableError> {
        // Find the first block that could contain `start`.
        let first_block = match self
            .index
            .binary_search_by(|e| e.first_key.as_slice().cmp(start))
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };

        let mut results = Vec::new();

        for ie in &self.index[first_block..] {
            // If we have an end bound and the block's first key is >= end,
            // all subsequent blocks are past the range.
            if let Some(end_key) = end {
                if ie.first_key.as_slice() >= end_key {
                    break;
                }
            }

            let entries = self.read_block(ie)?;
            for (k, v) in entries {
                if k.as_slice() < start {
                    continue;
                }
                if let Some(end_key) = end {
                    if k.as_slice() >= end_key {
                        return Ok(results);
                    }
                }
                results.push((k, v));
            }
        }

        Ok(results)
    }

    /// Search a single data block for an exact key match.
    fn search_block(
        &self,
        ie: &IndexEntry,
        key: &[u8],
    ) -> Result<Option<Option<Vec<u8>>>, SSTableError> {
        let entries = self.read_block(ie)?;
        for (k, v) in entries {
            match k.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(v)),
                std::cmp::Ordering::Greater => return Ok(None), // past it — sorted
                std::cmp::Ordering::Less => continue,
            }
        }
        Ok(None)
    }

    /// Decode all key-value pairs from a data block.
    fn read_block(
        &self,
        ie: &IndexEntry,
    ) -> Result<Vec<KvPair>, SSTableError> {
        let start = ie.offset as usize;
        let end = start + ie.length as usize;
        if end > self.data.len() {
            return Err(SSTableError::CorruptBlock {
                offset: ie.offset,
                reason: "block extends past file".to_string(),
            });
        }

        let mut cursor = &self.data[start..end];
        let mut entries = Vec::new();

        while cursor.len() >= 5 {
            // key_len (4) + value_flag (1)
            let key_len = u32::from_be_bytes(cursor[0..4].try_into().unwrap()) as usize;
            let value_flag = cursor[4];
            cursor = &cursor[5..];

            match value_flag {
                1 => {
                    // Live entry: value_len (4) + key + value
                    if cursor.len() < 4 {
                        return Err(SSTableError::CorruptBlock {
                            offset: ie.offset,
                            reason: "truncated value_len".to_string(),
                        });
                    }
                    let value_len =
                        u32::from_be_bytes(cursor[0..4].try_into().unwrap()) as usize;
                    cursor = &cursor[4..];

                    if cursor.len() < key_len + value_len {
                        return Err(SSTableError::CorruptBlock {
                            offset: ie.offset,
                            reason: "truncated key/value".to_string(),
                        });
                    }
                    let key = cursor[..key_len].to_vec();
                    let value = cursor[key_len..key_len + value_len].to_vec();
                    cursor = &cursor[key_len + value_len..];
                    entries.push((key, Some(value)));
                }
                0 => {
                    // Tombstone: key only
                    if cursor.len() < key_len {
                        return Err(SSTableError::CorruptBlock {
                            offset: ie.offset,
                            reason: "truncated tombstone key".to_string(),
                        });
                    }
                    let key = cursor[..key_len].to_vec();
                    cursor = &cursor[key_len..];
                    entries.push((key, None));
                }
                other => {
                    return Err(SSTableError::CorruptBlock {
                        offset: ie.offset,
                        reason: format!("unknown value_flag: {other}"),
                    });
                }
            }
        }

        Ok(entries)
    }

    /// Decode the index block.
    fn decode_index(mut data: &[u8]) -> Result<Vec<IndexEntry>, SSTableError> {
        let mut entries = Vec::new();
        while data.len() >= 4 {
            let key_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
            data = &data[4..];

            let needed = key_len + 8 + 4; // key + offset(u64) + length(u32)
            if data.len() < needed {
                return Err(SSTableError::CorruptIndex(
                    "truncated index entry".to_string(),
                ));
            }

            let first_key = data[..key_len].to_vec();
            data = &data[key_len..];

            let offset = u64::from_be_bytes(data[0..8].try_into().unwrap());
            let length = u32::from_be_bytes(data[8..12].try_into().unwrap());
            data = &data[12..];

            entries.push(IndexEntry {
                first_key,
                offset,
                length,
            });
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::writer::SSTableWriter;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("raft_db_sstable_tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{name}.sst"))
    }

    fn sample_entries(n: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        (0..n)
            .map(|i| {
                (
                    format!("key-{i:05}").into_bytes(),
                    Some(format!("val-{i:05}").into_bytes()),
                )
            })
            .collect()
    }

    fn write_and_open(name: &str, entries: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> SSTableReader {
        let path = temp_path(name);
        let _ = fs::remove_file(&path);
        let w = SSTableWriter::new(&path).with_block_size(128);
        let count = w.write(entries.iter().cloned()).unwrap();
        assert!(count > 0);
        SSTableReader::open(&path).unwrap()
    }

    #[test]
    fn open_validates_magic() {
        let path = temp_path("bad_magic");
        fs::write(&path, b"this is not an sstable at all!!x").unwrap();
        let result = SSTableReader::open(&path);
        assert!(matches!(result, Err(SSTableError::BadMagic)));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn open_rejects_tiny_file() {
        let path = temp_path("tiny_file");
        fs::write(&path, b"short").unwrap();
        let result = SSTableReader::open(&path);
        assert!(matches!(result, Err(SSTableError::BadMagic)));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn get_finds_existing_keys() {
        let entries = sample_entries(50);
        let reader = write_and_open("get_existing", entries.clone());

        assert_eq!(reader.entry_count(), 50);

        for (k, v) in &entries {
            let result = reader.get(k).unwrap();
            assert_eq!(result, Some(v.clone()), "key {:?}", String::from_utf8_lossy(k));
        }
    }

    #[test]
    fn get_returns_none_for_absent_key() {
        let entries = sample_entries(20);
        let reader = write_and_open("get_absent", entries);
        assert_eq!(reader.get(b"no-such-key").unwrap(), None);
    }

    #[test]
    fn get_returns_tombstone() {
        let entries = vec![
            (b"a".to_vec(), Some(b"alive".to_vec())),
            (b"b".to_vec(), None), // tombstone
            (b"c".to_vec(), Some(b"also alive".to_vec())),
        ];
        let reader = write_and_open("get_tombstone", entries);

        assert_eq!(reader.get(b"a").unwrap(), Some(Some(b"alive".to_vec())));
        assert_eq!(reader.get(b"b").unwrap(), Some(None)); // tombstone
        assert_eq!(reader.get(b"c").unwrap(), Some(Some(b"also alive".to_vec())));
    }

    #[test]
    fn scan_full_range() {
        let entries = sample_entries(30);
        let reader = write_and_open("scan_full", entries.clone());

        let result = reader.scan(b"key-00000", None).unwrap();
        assert_eq!(result.len(), 30);
        assert_eq!(result, entries);
    }

    #[test]
    fn scan_bounded_range() {
        let entries = sample_entries(100);
        let reader = write_and_open("scan_bounded", entries);

        let result = reader.scan(b"key-00010", Some(b"key-00020")).unwrap();
        assert_eq!(result.len(), 10);
        assert_eq!(
            String::from_utf8_lossy(&result[0].0),
            "key-00010"
        );
        assert_eq!(
            String::from_utf8_lossy(&result[9].0),
            "key-00019"
        );
    }

    #[test]
    fn scan_empty_range() {
        let entries = sample_entries(10);
        let reader = write_and_open("scan_empty", entries);

        let result = reader.scan(b"zzz", None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_includes_tombstones() {
        let entries = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), None),
            (b"c".to_vec(), Some(b"3".to_vec())),
        ];
        let reader = write_and_open("scan_tombstones", entries.clone());

        let result = reader.scan(b"a", Some(b"d")).unwrap();
        assert_eq!(result, entries);
    }

    #[test]
    fn many_entries_across_blocks() {
        // Use small block size to force many blocks.
        let path = temp_path("many_blocks");
        let _ = fs::remove_file(&path);

        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..500)
            .map(|i| {
                (
                    format!("k{i:06}").into_bytes(),
                    Some(format!("value-data-{i:06}").into_bytes()),
                )
            })
            .collect();

        let w = SSTableWriter::new(&path).with_block_size(64);
        w.write(entries.iter().cloned()).unwrap();

        let reader = SSTableReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 500);

        // Spot-check some keys.
        for &i in &[0, 1, 42, 250, 499] {
            let key = format!("k{i:06}").into_bytes();
            let expected = format!("value-data-{i:06}").into_bytes();
            assert_eq!(reader.get(&key).unwrap(), Some(Some(expected)));
        }

        // Range scan across block boundaries.
        let range = reader.scan(b"k000100", Some(b"k000200")).unwrap();
        assert_eq!(range.len(), 100);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn single_entry_sstable() {
        let entries = vec![(b"only".to_vec(), Some(b"one".to_vec()))];
        let reader = write_and_open("single_entry", entries);

        assert_eq!(reader.entry_count(), 1);
        assert_eq!(reader.get(b"only").unwrap(), Some(Some(b"one".to_vec())));
        assert_eq!(reader.get(b"other").unwrap(), None);
    }

    #[test]
    fn get_key_before_first_returns_none() {
        let entries = vec![
            (b"m".to_vec(), Some(b"mid".to_vec())),
            (b"z".to_vec(), Some(b"end".to_vec())),
        ];
        let reader = write_and_open("before_first", entries);
        assert_eq!(reader.get(b"a").unwrap(), None);
    }
}
