use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::bloom::BloomFilter;
use super::error::SSTableError;
use super::{DEFAULT_BLOCK_SIZE, SSTABLE_MAGIC};
use crate::crypto::Cipher;

/// Target bloom-filter false-positive rate. The filter is sized from the
/// actual key count of each table, so this rate holds regardless of how
/// many entries the table contains.
const BLOOM_FP_RATE: f64 = 0.01;

/// One restart point is recorded every this many entries. Point lookups
/// binary-search the restart array, then scan at most this many entries.
pub(crate) const RESTART_INTERVAL: usize = 16;

/// Builds an immutable SSTable file from a sorted iterator of key-value pairs.
pub struct SSTableWriter {
    path: PathBuf,
    block_size: usize,
    /// When set (F4 encryption at rest), each data block, the bloom filter,
    /// and the index block are sealed individually with AES-256-GCM. The
    /// 32-byte footer stays plaintext — it holds only structural offsets.
    cipher: Option<Arc<Cipher>>,
}

/// On-disk entry encoding inside a data block:
///
/// ```text
/// [key_len: u32][value_flag: u8][value_len: u32 (if flag=1)][key][value (if flag=1)]
/// ```
///
/// `value_flag` 0 = tombstone, 1 = live value.
///
/// Each block ends with a restart trailer enabling binary search:
///
/// ```text
/// [restart_offset: u32] × num_restarts  [num_restarts: u32]
/// ```
///
/// A restart offset is the byte position (within the block) of the entry
/// recorded every [`RESTART_INTERVAL`] entries; offset 0 is always present.
struct BlockBuilder {
    data: Vec<u8>,
    first_key: Option<Vec<u8>>,
    count: usize,
    /// Byte offsets of restart-point entries within `data`.
    restarts: Vec<u32>,
}

/// A finished block ready to be written, along with its first key.
struct FinishedBlock {
    data: Vec<u8>,
    first_key: Vec<u8>,
}

/// One entry in the index block: first_key → (offset, length).
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    length: u32,
}

impl BlockBuilder {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            first_key: None,
            count: 0,
            restarts: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn add(&mut self, key: &[u8], value: &Option<Vec<u8>>) {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }

        if self.count % RESTART_INTERVAL == 0 {
            self.restarts.push(self.data.len() as u32);
        }

        // key_len
        self.data
            .extend_from_slice(&(key.len() as u32).to_be_bytes());

        match value {
            Some(v) => {
                self.data.push(1); // live
                self.data.extend_from_slice(&(v.len() as u32).to_be_bytes());
                self.data.extend_from_slice(key);
                self.data.extend_from_slice(v);
            }
            None => {
                self.data.push(0); // tombstone
                                   // no value_len for tombstones
                self.data.extend_from_slice(key);
            }
        }

        self.count += 1;
    }

    fn finish(self) -> Option<FinishedBlock> {
        let first_key = self.first_key?;
        // Append the restart trailer: offsets then the count.
        let mut data = self.data;
        data.reserve(4 * (self.restarts.len() + 1));
        for r in &self.restarts {
            data.extend_from_slice(&r.to_be_bytes());
        }
        data.extend_from_slice(&(self.restarts.len() as u32).to_be_bytes());
        Some(FinishedBlock { data, first_key })
    }
}

impl SSTableWriter {
    /// Create a writer targeting `path` with the default block size.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            block_size: DEFAULT_BLOCK_SIZE,
            cipher: None,
        }
    }

    /// Override the target data block size (bytes). Mainly useful for tests.
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size.max(64);
        self
    }

    /// Enable encryption at rest: every region except the footer is sealed.
    pub fn with_cipher(mut self, cipher: Option<Arc<Cipher>>) -> Self {
        self.cipher = cipher;
        self
    }

    /// Seal `data` when a cipher is configured, otherwise pass it through.
    fn maybe_seal(&self, data: Vec<u8>) -> Result<Vec<u8>, SSTableError> {
        match &self.cipher {
            Some(cipher) => cipher
                .seal(&data)
                .map_err(|e| SSTableError::Encryption(e.to_string())),
            None => Ok(data),
        }
    }

    /// Write a complete SSTable from a **sorted** iterator of key-value pairs.
    ///
    /// The iterator must yield `(key, Option<value>)` in ascending key order.
    /// `None` values represent tombstones.
    ///
    /// Returns the number of entries written.
    pub fn write(
        &self,
        iter: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<usize, SSTableError> {
        let mut file = BufWriter::new(File::create(&self.path)?);
        let mut index_entries: Vec<IndexEntry> = Vec::new();
        // Key hashes are collected while streaming so the bloom filter can
        // be sized for the *actual* entry count once it is known (a fixed
        // pre-sized filter degrades badly past its expected key count).
        let mut key_hashes: Vec<u64> = Vec::new();
        let mut block = BlockBuilder::new();
        let mut current_offset: u64 = 0;
        let mut total_entries: usize = 0;

        for (key, value) in iter {
            key_hashes.push(BloomFilter::hash64(&key));
            block.add(&key, &value);
            total_entries += 1;

            if block.estimated_size() >= self.block_size {
                let finished = block.finish().expect("block is non-empty");
                let data = self.maybe_seal(finished.data)?;
                let len = data.len() as u32;
                file.write_all(&data)?;
                index_entries.push(IndexEntry {
                    first_key: finished.first_key,
                    offset: current_offset,
                    length: len,
                });
                current_offset += len as u64;
                block = BlockBuilder::new();
            }
        }

        // Flush the last partial block.
        if !block.is_empty() {
            let finished = block.finish().expect("block is non-empty");
            let data = self.maybe_seal(finished.data)?;
            let len = data.len() as u32;
            file.write_all(&data)?;
            index_entries.push(IndexEntry {
                first_key: finished.first_key,
                offset: current_offset,
                length: len,
            });
            current_offset += len as u64;
        }

        if total_entries == 0 {
            // Clean up the empty file we created.
            drop(file);
            std::fs::remove_file(&self.path).ok();
            return Err(SSTableError::EmptyInput);
        }

        // ── Bloom filter — sized to the exact key count ──
        let bloom_offset = current_offset;
        let bloom = BloomFilter::from_hashes(&key_hashes, BLOOM_FP_RATE);
        drop(key_hashes);
        let bloom_data = self.maybe_seal(bloom.encode())?;
        file.write_all(&bloom_data)?;
        current_offset += bloom_data.len() as u64;

        // ── Index block ──
        let index_offset = current_offset;
        let index_data = self.maybe_seal(Self::encode_index(&index_entries))?;
        file.write_all(&index_data)?;

        // ── Footer (32 bytes) ──
        // [bloom_offset: u64][index_offset: u64][entry_count: u64][reserved: u32][magic: 4]
        let mut footer = [0u8; 32];
        footer[0..8].copy_from_slice(&bloom_offset.to_be_bytes());
        footer[8..16].copy_from_slice(&index_offset.to_be_bytes());
        footer[16..24].copy_from_slice(&(total_entries as u64).to_be_bytes());
        // bytes 24..28 reserved (zero)
        footer[28..32].copy_from_slice(&SSTABLE_MAGIC);
        file.write_all(&footer)?;

        file.flush()?;
        // fsync before the table is registered in the manifest — a crash
        // must never leave the manifest referencing a partially-written file.
        file.get_ref().sync_all()?;
        Ok(total_entries)
    }

    /// Encode index entries into a byte buffer.
    ///
    /// Layout per entry: `[key_len: u32][key][offset: u64][length: u32]`
    fn encode_index(entries: &[IndexEntry]) -> Vec<u8> {
        let mut buf = Vec::new();
        for e in entries {
            buf.extend_from_slice(&(e.first_key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&e.first_key);
            buf.extend_from_slice(&e.offset.to_be_bytes());
            buf.extend_from_slice(&e.length.to_be_bytes());
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("raft_db_sstable_tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{name}.sst"))
    }

    #[test]
    fn write_creates_file_with_magic() {
        let path = temp_path("write_magic");
        let _ = fs::remove_file(&path);

        let entries = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), Some(b"2".to_vec())),
        ];

        let w = SSTableWriter::new(&path).with_block_size(64);
        let count = w.write(entries.into_iter()).unwrap();
        assert_eq!(count, 2);

        let data = fs::read(&path).unwrap();
        assert!(data.len() >= 32);
        assert_eq!(&data[data.len() - 4..], &SSTABLE_MAGIC);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn write_empty_iterator_returns_error() {
        let path = temp_path("write_empty");
        let _ = fs::remove_file(&path);

        let w = SSTableWriter::new(&path);
        let result = w.write(std::iter::empty());
        assert!(matches!(result, Err(SSTableError::EmptyInput)));
    }

    #[test]
    fn write_includes_tombstones() {
        let path = temp_path("write_tombstone");
        let _ = fs::remove_file(&path);

        let entries = vec![
            (b"alive".to_vec(), Some(b"yes".to_vec())),
            (b"dead".to_vec(), None),
        ];

        let w = SSTableWriter::new(&path).with_block_size(64);
        let count = w.write(entries.into_iter()).unwrap();
        assert_eq!(count, 2);

        fs::remove_file(&path).ok();
    }
}
