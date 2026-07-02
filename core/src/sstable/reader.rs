use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::bloom::BloomFilter;
use super::cache::BlockCache;
use super::error::SSTableError;
use super::SSTABLE_MAGIC;

/// A key-value pair where `None` value represents a tombstone.
type KvPair = (Vec<u8>, Option<Vec<u8>>);

/// Footer size in bytes.
const FOOTER_SIZE: usize = 32;

/// Reads an immutable SSTable file.
///
/// On `open`, only the footer, index block, and bloom filter are loaded
/// into memory. Data blocks are read on demand (positional reads) during
/// `get` and `scan`, optionally through a shared [`BlockCache`] when the
/// reader was opened via [`SSTableReader::open_with_cache`].
pub struct SSTableReader {
    path: PathBuf,
    /// Open handle used for positional data-block reads.
    file: fs::File,
    /// Total file length, used to bounds-check block reads.
    file_len: u64,
    bloom: BloomFilter,
    index: Vec<IndexEntry>,
    entry_count: u64,
    /// Shared block cache plus the table id used as the cache key namespace.
    /// `None` means every block read hits the file (used by compaction so
    /// one-shot merges don't pollute the cache).
    cache: Option<(Arc<BlockCache>, u64)>,
}

/// Decoded index entry: first key of a data block and where to find it.
#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    length: u32,
}

impl SSTableReader {
    /// Open an SSTable file without a block cache. Every data-block read
    /// goes to the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SSTableError> {
        Self::open_inner(path.as_ref(), None)
    }

    /// Open an SSTable file whose data-block reads go through `cache`,
    /// keyed by `table_id`.
    pub fn open_with_cache(
        path: impl AsRef<Path>,
        cache: Arc<BlockCache>,
        table_id: u64,
    ) -> Result<Self, SSTableError> {
        Self::open_inner(path.as_ref(), Some((cache, table_id)))
    }

    fn open_inner(
        path: &Path,
        cache: Option<(Arc<BlockCache>, u64)>,
    ) -> Result<Self, SSTableError> {
        let path = path.to_path_buf();
        let file = fs::File::open(&path)?;
        let file_len = file.metadata()?.len();

        if (file_len as usize) < FOOTER_SIZE {
            return Err(SSTableError::BadMagic);
        }

        // ── Parse footer ──
        let footer_start = file_len - FOOTER_SIZE as u64;
        let mut footer = [0u8; FOOTER_SIZE];
        read_exact_at(&file, &mut footer, footer_start)?;

        let magic = &footer[28..32];
        if magic != SSTABLE_MAGIC {
            return Err(SSTableError::BadMagic);
        }

        let bloom_offset = u64::from_be_bytes(footer[0..8].try_into().unwrap());
        let index_offset = u64::from_be_bytes(footer[8..16].try_into().unwrap());
        let entry_count = u64::from_be_bytes(footer[16..24].try_into().unwrap());

        if bloom_offset > index_offset || index_offset > footer_start {
            return Err(SSTableError::CorruptIndex(
                "offsets out of range".to_string(),
            ));
        }

        // ── Load bloom filter + index block in one read ──
        let meta_len = (footer_start - bloom_offset) as usize;
        let mut meta = vec![0u8; meta_len];
        read_exact_at(&file, &mut meta, bloom_offset)?;

        let bloom_len = (index_offset - bloom_offset) as usize;
        let bloom = BloomFilter::decode(&meta[..bloom_len]).ok_or_else(|| {
            SSTableError::CorruptIndex("failed to decode bloom filter".to_string())
        })?;
        let index = Self::decode_index(&meta[bloom_len..])?;

        Ok(Self {
            path,
            file,
            file_len,
            bloom,
            index,
            entry_count,
            cache,
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

    /// Read every entry in sorted order into memory.
    ///
    /// Prefer [`SSTableReader::iter`] for large tables — it holds only
    /// one decoded block at a time.
    pub fn scan_all(&self) -> Result<Vec<KvPair>, SSTableError> {
        let mut all = Vec::new();
        for ie in &self.index {
            let block = self.read_block_bytes(ie)?;
            all.extend(Self::decode_block(&block, ie.offset)?);
        }
        Ok(all)
    }

    /// Streaming iterator over every entry in sorted order, decoding one
    /// data block at a time. Peak memory is a single block regardless of
    /// table size — used by compaction to merge tables without loading
    /// them fully into RAM.
    pub fn iter(&self) -> SSTableIter<'_> {
        SSTableIter {
            reader: self,
            block_idx: 0,
            entries: Vec::new().into_iter(),
        }
    }

    /// Point lookup — returns `Some(Some(value))` for a live key,
    /// `Some(None)` for a tombstone, or `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, SSTableError> {
        // Fast path: bloom filter rejects definitely-absent keys.
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Find the candidate block via binary search on the index.
        let block_idx = match self
            .index
            .binary_search_by(|e| e.first_key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => return Ok(None), // key is before the first block
            Err(i) => i - 1,
        };

        let ie = &self.index[block_idx];
        let block = self.read_block_bytes(ie)?;
        Self::search_block(&block, ie.offset, key)
    }

    /// Range scan — returns all entries with `start <= key < end` in sorted
    /// order. Both bounds are byte-slice keys. If `end` is `None`, scans to
    /// the end of the table.
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>) -> Result<Vec<KvPair>, SSTableError> {
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

            let block = self.read_block_bytes(ie)?;
            let entries = Self::decode_block(&block, ie.offset)?;
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

    /// Fetch a data block's raw bytes, consulting the shared cache first
    /// when one is configured.
    fn read_block_bytes(&self, ie: &IndexEntry) -> Result<Arc<Vec<u8>>, SSTableError> {
        let end = ie.offset + ie.length as u64;
        if end > self.file_len {
            return Err(SSTableError::CorruptBlock {
                offset: ie.offset,
                reason: "block extends past file".to_string(),
            });
        }

        if let Some((cache, table_id)) = &self.cache {
            if let Some(block) = cache.get(*table_id, ie.offset) {
                return Ok(block);
            }
        }

        let mut buf = vec![0u8; ie.length as usize];
        read_exact_at(&self.file, &mut buf, ie.offset)?;
        let block = Arc::new(buf);

        if let Some((cache, table_id)) = &self.cache {
            cache.insert(*table_id, ie.offset, Arc::clone(&block));
        }
        Ok(block)
    }

    /// Search a single data block for an exact key match.
    ///
    /// Streams over the block bytes without materialising a `Vec` of
    /// owned key/value pairs. Only the matching value is allocated; all
    /// other entries are skipped in place.
    fn search_block(
        block: &[u8],
        block_offset: u64,
        key: &[u8],
    ) -> Result<Option<Option<Vec<u8>>>, SSTableError> {
        let mut cursor = block;
        while cursor.len() >= 5 {
            let key_len = u32::from_be_bytes(cursor[0..4].try_into().unwrap()) as usize;
            let value_flag = cursor[4];
            cursor = &cursor[5..];

            match value_flag {
                1 => {
                    if cursor.len() < 4 {
                        return Err(SSTableError::CorruptBlock {
                            offset: block_offset,
                            reason: "truncated value_len".to_string(),
                        });
                    }
                    let value_len = u32::from_be_bytes(cursor[0..4].try_into().unwrap()) as usize;
                    cursor = &cursor[4..];
                    if cursor.len() < key_len + value_len {
                        return Err(SSTableError::CorruptBlock {
                            offset: block_offset,
                            reason: "truncated key/value".to_string(),
                        });
                    }
                    let entry_key = &cursor[..key_len];
                    match entry_key.cmp(key) {
                        std::cmp::Ordering::Equal => {
                            let value = cursor[key_len..key_len + value_len].to_vec();
                            return Ok(Some(Some(value)));
                        }
                        std::cmp::Ordering::Greater => return Ok(None),
                        std::cmp::Ordering::Less => {
                            cursor = &cursor[key_len + value_len..];
                        }
                    }
                }
                0 => {
                    if cursor.len() < key_len {
                        return Err(SSTableError::CorruptBlock {
                            offset: block_offset,
                            reason: "truncated tombstone key".to_string(),
                        });
                    }
                    let entry_key = &cursor[..key_len];
                    match entry_key.cmp(key) {
                        std::cmp::Ordering::Equal => return Ok(Some(None)),
                        std::cmp::Ordering::Greater => return Ok(None),
                        std::cmp::Ordering::Less => {
                            cursor = &cursor[key_len..];
                        }
                    }
                }
                other => {
                    return Err(SSTableError::CorruptBlock {
                        offset: block_offset,
                        reason: format!("unknown value_flag: {other}"),
                    });
                }
            }
        }
        Ok(None)
    }

    /// Decode all key-value pairs from a data block.
    fn decode_block(block: &[u8], block_offset: u64) -> Result<Vec<KvPair>, SSTableError> {
        let mut cursor = block;
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
                            offset: block_offset,
                            reason: "truncated value_len".to_string(),
                        });
                    }
                    let value_len = u32::from_be_bytes(cursor[0..4].try_into().unwrap()) as usize;
                    cursor = &cursor[4..];

                    if cursor.len() < key_len + value_len {
                        return Err(SSTableError::CorruptBlock {
                            offset: block_offset,
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
                            offset: block_offset,
                            reason: "truncated tombstone key".to_string(),
                        });
                    }
                    let key = cursor[..key_len].to_vec();
                    cursor = &cursor[key_len..];
                    entries.push((key, None));
                }
                other => {
                    return Err(SSTableError::CorruptBlock {
                        offset: block_offset,
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

/// Streaming iterator over an SSTable's entries in sorted key order.
///
/// Decodes one data block at a time, so peak memory is one block plus
/// the entries decoded from it. Created via [`SSTableReader::iter`].
pub struct SSTableIter<'a> {
    reader: &'a SSTableReader,
    /// Next index-block entry to decode.
    block_idx: usize,
    /// Entries of the current block, drained front to back.
    entries: std::vec::IntoIter<KvPair>,
}

impl Iterator for SSTableIter<'_> {
    type Item = Result<KvPair, SSTableError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(kv) = self.entries.next() {
                return Some(Ok(kv));
            }
            if self.block_idx >= self.reader.index.len() {
                return None;
            }
            let ie = &self.reader.index[self.block_idx];
            self.block_idx += 1;
            match self
                .reader
                .read_block_bytes(ie)
                .and_then(|b| SSTableReader::decode_block(&b, ie.offset))
            {
                Ok(entries) => self.entries = entries.into_iter(),
                Err(e) => {
                    // Poison the iterator: no further blocks after an error.
                    self.block_idx = self.reader.index.len();
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Positional read that fills `buf` from `offset` without moving a shared
/// file cursor — safe for concurrent readers over the same handle.
#[cfg(unix)]
fn read_exact_at(file: &fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

/// Positional read that fills `buf` from `offset` without moving a shared
/// file cursor — safe for concurrent readers over the same handle.
#[cfg(windows)]
fn read_exact_at(file: &fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut buf = buf;
    let mut offset = offset;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
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
            assert_eq!(
                result,
                Some(v.clone()),
                "key {:?}",
                String::from_utf8_lossy(k)
            );
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
        assert_eq!(
            reader.get(b"c").unwrap(),
            Some(Some(b"also alive".to_vec()))
        );
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
        assert_eq!(String::from_utf8_lossy(&result[0].0), "key-00010");
        assert_eq!(String::from_utf8_lossy(&result[9].0), "key-00019");
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

    #[test]
    fn iter_streams_all_entries_in_order() {
        let entries = sample_entries(200);
        let reader = write_and_open("iter_streams", entries.clone());

        let collected: Vec<_> = reader.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(collected, entries);
    }

    #[test]
    fn iter_includes_tombstones() {
        let entries = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), None),
            (b"c".to_vec(), Some(b"3".to_vec())),
        ];
        let reader = write_and_open("iter_tombstones", entries.clone());

        let collected: Vec<_> = reader.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(collected, entries);
    }

    #[test]
    fn cached_reader_returns_correct_values_and_populates_cache() {
        let path = temp_path("cached_reader");
        let _ = fs::remove_file(&path);

        let entries = sample_entries(100);
        let w = SSTableWriter::new(&path).with_block_size(128);
        w.write(entries.iter().cloned()).unwrap();

        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let reader = SSTableReader::open_with_cache(&path, Arc::clone(&cache), 7).unwrap();

        for (k, v) in &entries {
            assert_eq!(reader.get(k).unwrap(), Some(v.clone()));
        }
        assert!(cache.current_bytes() > 0, "cache must hold blocks");

        // Second pass must be served from cache (hits increase).
        let hits_before = cache.hits();
        for (k, v) in &entries {
            assert_eq!(reader.get(k).unwrap(), Some(v.clone()));
        }
        assert!(cache.hits() > hits_before, "repeat reads must hit cache");

        fs::remove_file(&path).ok();
    }

    #[test]
    fn cached_reader_correct_under_tiny_cache_eviction() {
        let path = temp_path("tiny_cache");
        let _ = fs::remove_file(&path);

        let entries = sample_entries(200);
        let w = SSTableWriter::new(&path).with_block_size(64);
        w.write(entries.iter().cloned()).unwrap();

        // Cap far smaller than the data set — constant eviction churn.
        let cache = Arc::new(BlockCache::new(256));
        let reader = SSTableReader::open_with_cache(&path, Arc::clone(&cache), 1).unwrap();

        for (k, v) in &entries {
            assert_eq!(reader.get(k).unwrap(), Some(v.clone()));
            assert!(cache.current_bytes() <= cache.capacity_bytes());
        }

        let scanned = reader.scan(b"key-00000", None).unwrap();
        assert_eq!(scanned, entries);
        assert!(cache.current_bytes() <= cache.capacity_bytes());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn cache_shared_across_readers_stays_bounded() {
        let cache = Arc::new(BlockCache::new(512));
        let mut readers = Vec::new();

        for t in 0..4u64 {
            let path = temp_path(&format!("shared_cache_{t}"));
            let _ = fs::remove_file(&path);
            let entries = sample_entries(50);
            let w = SSTableWriter::new(&path).with_block_size(64);
            w.write(entries.iter().cloned()).unwrap();
            readers.push((
                SSTableReader::open_with_cache(&path, Arc::clone(&cache), t).unwrap(),
                entries,
                path,
            ));
        }

        for (reader, entries, _) in &readers {
            for (k, v) in entries {
                assert_eq!(reader.get(k).unwrap(), Some(v.clone()));
            }
        }
        assert!(cache.current_bytes() <= cache.capacity_bytes());

        // Evicting one table keeps the others readable.
        cache.evict_table(0);
        let (reader, entries, _) = &readers[1];
        for (k, v) in entries {
            assert_eq!(reader.get(k).unwrap(), Some(v.clone()));
        }

        for (_, _, path) in &readers {
            fs::remove_file(path).ok();
        }
    }
}
