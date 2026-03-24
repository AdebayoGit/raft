//! StorageEngine — wires WAL, MemTable, SSTable, Compaction, and Manifest
//! into a single cohesive read/write interface.
//!
//! Write path:  put/delete → WAL append → MemTable insert → flush to SSTable if full
//! Read path:   get → MemTable → SSTables (newest first, L0 → Lmax)

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compaction::CompactionConfig;
use crate::manifest::{Manifest, SSTableMeta, TableId};
use crate::memtable::MemTable;
use crate::sstable::{SSTableReader, SSTableWriter};
use crate::wal::{HlcTimestamp, Wal, WalEntry};

/// Unified error type for the storage engine.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("WAL error: {0}")]
    Wal(#[from] crate::wal::WalError),

    #[error("SSTable error: {0}")]
    SSTable(#[from] crate::sstable::SSTableError),

    #[error("compaction error: {0}")]
    Compaction(#[from] crate::compaction::CompactionError),

    #[error("manifest error: {0}")]
    Manifest(#[from] crate::manifest::ManifestError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for the storage engine.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Maximum MemTable size in bytes before flushing to SSTable.
    /// Default: 4 MiB.
    pub memtable_size: usize,
    /// Target SSTable data block size. Default: 4096.
    pub block_size: usize,
    /// Compaction configuration.
    pub compaction: CompactionConfig,
    /// Device ID for WAL entries (128-bit UUID).
    /// Default: 0 (single-device / testing).
    pub device_id: u128,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            block_size: 4096,
            compaction: CompactionConfig::default(),
            device_id: 0,
        }
    }
}

// ── WAL payload encoding ──
// Put:    [0x01][key_len: u32 BE][key][value_len: u32 BE][value]
// Delete: [0x02][key_len: u32 BE][key]

const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;

fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    buf.push(OP_PUT);
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    buf
}

fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + key.len());
    buf.push(OP_DELETE);
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Decoded WAL operation for replay.
enum WalOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

fn decode_payload(payload: &[u8]) -> Option<WalOp> {
    if payload.is_empty() {
        return None;
    }
    let op = payload[0];
    let rest = &payload[1..];
    match op {
        OP_PUT => {
            if rest.len() < 4 {
                return None;
            }
            let key_len = u32::from_be_bytes(rest[0..4].try_into().ok()?) as usize;
            let rest = &rest[4..];
            if rest.len() < key_len + 4 {
                return None;
            }
            let key = rest[..key_len].to_vec();
            let rest = &rest[key_len..];
            let val_len = u32::from_be_bytes(rest[0..4].try_into().ok()?) as usize;
            let rest = &rest[4..];
            if rest.len() < val_len {
                return None;
            }
            let value = rest[..val_len].to_vec();
            Some(WalOp::Put { key, value })
        }
        OP_DELETE => {
            if rest.len() < 4 {
                return None;
            }
            let key_len = u32::from_be_bytes(rest[0..4].try_into().ok()?) as usize;
            let rest = &rest[4..];
            if rest.len() < key_len {
                return None;
            }
            let key = rest[..key_len].to_vec();
            Some(WalOp::Delete { key })
        }
        _ => None,
    }
}

/// The main storage engine. Coordinates all subsystems.
pub struct StorageEngine {
    db_dir: PathBuf,
    config: StorageConfig,
    wal: Wal,
    memtable: MemTable,
    manifest: Manifest,
    /// Monotonically increasing sequence number.
    sequence: u64,
    /// Logical HLC counter for events within the same millisecond.
    hlc_logical: u16,
    /// Last physical timestamp seen, for HLC advancement.
    hlc_physical: u64,
    /// Next unique SSTable ID.
    next_table_id: TableId,
}

impl StorageEngine {
    /// Open or create a database at `db_dir`.
    ///
    /// On open: replays the manifest to learn which SSTables are live,
    /// then replays the WAL to recover any unflushed memtable state.
    pub fn open(
        db_dir: impl AsRef<Path>,
        config: StorageConfig,
    ) -> Result<Self, StorageError> {
        let db_dir = db_dir.as_ref().to_path_buf();
        fs::create_dir_all(&db_dir)?;

        // Ensure level directories exist.
        let sstables_dir = db_dir.join("sstables");
        for l in 0..config.compaction.max_levels {
            fs::create_dir_all(sstables_dir.join(format!("L{l}")))?;
        }

        // Open manifest.
        let manifest_path = db_dir.join("MANIFEST");
        let manifest = Manifest::open(&manifest_path)?;
        let version = manifest.current_version();

        // Derive next table ID from existing tables.
        let next_table_id = version
            .tables
            .keys()
            .last()
            .map_or(1, |max_id| max_id + 1);

        // Open WAL.
        let wal_path = db_dir.join("wal.log");
        let wal = Wal::open(&wal_path)?;

        // Create memtable and replay WAL.
        let mut memtable = MemTable::new(config.memtable_size);
        let replay_iter = wal.replay()?;
        for entry_result in replay_iter {
            let entry = entry_result?;
            if let Some(op) = decode_payload(&entry.payload) {
                match op {
                    WalOp::Put { key, value } => memtable.insert(key, value),
                    WalOp::Delete { key } => memtable.delete(key),
                }
            }
        }

        Ok(Self {
            db_dir,
            config,
            wal,
            memtable,
            manifest,
            sequence: version.sequence,
            hlc_logical: 0,
            hlc_physical: 0,
            next_table_id,
        })
    }

    /// Insert or update a key-value pair.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StorageError> {
        let ts = self.advance_hlc();
        let payload = encode_put(&key, &value);
        let entry = WalEntry::new(ts, self.config.device_id, payload);
        self.wal.append(&entry)?;

        self.memtable.insert(key, value);
        self.sequence += 1;

        if self.memtable.should_flush() {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Look up a key. Returns `Some(value)` if live, `None` if absent or
    /// deleted.
    ///
    /// Read path: MemTable → SSTables (newest first, L0 → Lmax).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        // 1. Check memtable.
        if let Some(maybe_value) = self.memtable.get(key) {
            return match maybe_value {
                Some(v) => Ok(Some(v.to_vec())),
                None => Ok(None), // tombstone
            };
        }

        // 2. Check SSTables from newest to oldest.
        //    Sort by level ascending, then by id descending within each level.
        let version = self.manifest.current_version();
        let mut tables: Vec<&SSTableMeta> = version.tables.values().collect();
        tables.sort_by(|a, b| a.level.cmp(&b.level).then_with(|| b.id.cmp(&a.id)));

        for meta in tables {
            // Quick key-range filter.
            if key < meta.smallest_key.as_slice() || key > meta.largest_key.as_slice() {
                continue;
            }

            let path = self.sstable_path(meta.id, meta.level);
            if !path.exists() {
                continue;
            }

            let reader = SSTableReader::open(&path)?;
            if let Some(maybe_value) = reader.get(key)? {
                return match maybe_value {
                    Some(v) => Ok(Some(v)),
                    None => Ok(None), // tombstone
                };
            }
        }

        Ok(None)
    }

    /// Delete a key by writing a tombstone.
    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), StorageError> {
        let ts = self.advance_hlc();
        let payload = encode_delete(&key);
        let entry = WalEntry::new(ts, self.config.device_id, payload);
        self.wal.append(&entry)?;

        self.memtable.delete(key);
        self.sequence += 1;

        if self.memtable.should_flush() {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Force-flush the current memtable to an SSTable, even if below
    /// the size threshold. No-op if the memtable is empty.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        if !self.memtable.is_empty() {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Run one compaction pass. Merges all SSTables at the first level
    /// that exceeds its threshold into a single table promoted one level up.
    ///
    /// Designed to be called when the system detects low activity.
    /// Does nothing if no level needs compaction.
    pub fn compact(&mut self) -> Result<CompactionStats, StorageError> {
        let mut stats = CompactionStats::default();
        let max_levels = self.config.compaction.max_levels;
        let threshold = self.config.compaction.level_threshold;

        for level in 0..(max_levels.saturating_sub(1)) {
            let level_u32 = level as u32;
            let tables_at_level = self.manifest.tables_at_level(level_u32);
            if tables_at_level.len() >= threshold {
                self.compact_level(level_u32, &mut stats)?;
                break;
            }
        }

        Ok(stats)
    }

    /// Current DB sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Path to the database directory.
    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    // ── Internal helpers ──

    /// Compact all SSTables at `level` into one at `level + 1`.
    fn compact_level(
        &mut self,
        level: u32,
        stats: &mut CompactionStats,
    ) -> Result<(), StorageError> {
        let next_level = level + 1;
        let tables: Vec<SSTableMeta> = self
            .manifest
            .tables_at_level(level)
            .into_iter()
            .cloned()
            .collect();

        if tables.is_empty() {
            return Ok(());
        }

        // Merge entries from all tables. Tables are sorted by id ascending
        // in the manifest (oldest first). For each key, the highest-id
        // (newest) entry wins.
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();

        for meta in &tables {
            let path = self.sstable_path(meta.id, meta.level);
            let reader = SSTableReader::open(&path)?;
            for (k, v) in reader.scan_all()? {
                merged.insert(k, v);
            }
        }

        let all_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = merged.into_iter().collect();

        if all_entries.is_empty() {
            return Ok(());
        }

        // Write merged SSTable.
        let new_id = self.next_table_id;
        self.next_table_id += 1;

        let smallest_key = all_entries.first().map(|(k, _)| k.clone()).unwrap_or_default();
        let largest_key = all_entries.last().map(|(k, _)| k.clone()).unwrap_or_default();
        let entry_count = all_entries.len() as u64;

        let out_path = self.sstable_path(new_id, next_level);
        let writer = SSTableWriter::new(&out_path).with_block_size(self.config.block_size);
        writer.write(all_entries.into_iter())?;

        let file_size = fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0);
        stats.tables_written += 1;

        // Register the new table in manifest.
        let new_meta = SSTableMeta {
            id: new_id,
            level: next_level,
            smallest_key,
            largest_key,
            entry_count,
            file_size,
        };
        self.manifest.add_sstable(new_meta)?;

        // Remove old tables from manifest and delete files.
        for meta in &tables {
            self.manifest.remove_sstable(meta.id)?;
            let old_path = self.sstable_path(meta.id, meta.level);
            fs::remove_file(&old_path).ok();
            stats.tables_deleted += 1;
        }
        stats.tables_merged += tables.len();
        stats.levels_compacted += 1;

        Ok(())
    }

    /// Flush the current memtable to a new L0 SSTable.
    fn flush_memtable(&mut self) -> Result<(), StorageError> {
        let table_id = self.next_table_id;
        self.next_table_id += 1;

        // Swap in a fresh memtable.
        let old = std::mem::replace(
            &mut self.memtable,
            MemTable::new(self.config.memtable_size),
        );

        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = old.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }

        let smallest_key = entries.first().map(|(k, _)| k.clone()).unwrap_or_default();
        let largest_key = entries.last().map(|(k, _)| k.clone()).unwrap_or_default();
        let entry_count = entries.len() as u64;

        let path = self.sstable_path(table_id, 0);
        let writer = SSTableWriter::new(&path).with_block_size(self.config.block_size);
        writer.write(entries.into_iter())?;

        let file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let meta = SSTableMeta {
            id: table_id,
            level: 0,
            smallest_key,
            largest_key,
            entry_count,
            file_size,
        };
        self.manifest.add_sstable(meta)?;
        self.manifest.set_sequence(self.sequence)?;

        // Truncate WAL — memtable data is now durable in the SSTable.
        let wal_path = self.db_dir.join("wal.log");
        fs::write(&wal_path, b"")?;
        self.wal = Wal::open(&wal_path)?;

        Ok(())
    }

    /// Generate the on-disk path for an SSTable.
    fn sstable_path(&self, id: TableId, level: u32) -> PathBuf {
        self.db_dir
            .join("sstables")
            .join(format!("L{level}"))
            .join(format!("{id:06}.sst"))
    }

    /// Advance the hybrid logical clock.
    fn advance_hlc(&mut self) -> HlcTimestamp {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if now_ms > self.hlc_physical {
            self.hlc_physical = now_ms;
            self.hlc_logical = 0;
        } else {
            self.hlc_logical = self.hlc_logical.wrapping_add(1);
        }

        HlcTimestamp::new(self.hlc_physical, self.hlc_logical)
    }
}

/// Statistics returned after a compaction pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub levels_compacted: usize,
    pub tables_merged: usize,
    pub tables_written: usize,
    pub tables_deleted: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("raft_db_engine_tests")
            .join(name);
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        dir
    }

    fn default_config() -> StorageConfig {
        StorageConfig {
            memtable_size: 4096,
            block_size: 128,
            compaction: CompactionConfig {
                level_threshold: 4,
                max_levels: 4,
                block_size: 128,
            },
            device_id: 0xDEAD,
        }
    }

    #[test]
    fn open_creates_directory_structure() {
        let dir = temp_dir("open_dirs");
        let _engine = StorageEngine::open(&dir, default_config()).unwrap();

        assert!(dir.join("MANIFEST").exists());
        assert!(dir.join("wal.log").exists());
        assert!(dir.join("sstables").is_dir());
        assert!(dir.join("sstables/L0").is_dir());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_and_get() {
        let dir = temp_dir("put_get");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();

        engine.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(engine.get(b"missing").unwrap(), None);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_overwrites() {
        let dir = temp_dir("put_overwrite");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();

        engine.put(b"key".to_vec(), b"old".to_vec()).unwrap();
        engine.put(b"key".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"new".to_vec()));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_removes_key() {
        let dir = temp_dir("delete");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();

        engine.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));

        engine.delete(b"key".to_vec()).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), None);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_nonexistent_is_ok() {
        let dir = temp_dir("delete_missing");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();

        engine.delete(b"ghost".to_vec()).unwrap();
        assert_eq!(engine.get(b"ghost").unwrap(), None);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn memtable_flush_to_sstable() {
        let dir = temp_dir("flush");
        let config = StorageConfig {
            memtable_size: 100,
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        for i in 0u32..20 {
            engine
                .put(format!("k{i:04}").into_bytes(), format!("v{i:04}").into_bytes())
                .unwrap();
        }

        for i in 0u32..20 {
            let val = engine.get(format!("k{i:04}").as_bytes()).unwrap();
            assert_eq!(val, Some(format!("v{i:04}").into_bytes()), "key k{i:04}");
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_from_wal() {
        let dir = temp_dir("wal_recovery");

        {
            let mut engine = StorageEngine::open(&dir, default_config()).unwrap();
            engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
            engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
            engine.delete(b"c".to_vec()).unwrap();
        }

        {
            let engine = StorageEngine::open(&dir, default_config()).unwrap();
            assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
            assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
            assert_eq!(engine.get(b"c").unwrap(), None);
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovery_from_sstable_after_flush() {
        let dir = temp_dir("sst_recovery");
        let config = StorageConfig {
            memtable_size: 50,
            ..default_config()
        };

        {
            let mut engine = StorageEngine::open(&dir, config.clone()).unwrap();
            for i in 0u32..30 {
                engine
                    .put(format!("k{i:04}").into_bytes(), format!("v{i:04}").into_bytes())
                    .unwrap();
            }
        }

        {
            let engine = StorageEngine::open(&dir, config).unwrap();
            for i in 0u32..30 {
                let val = engine.get(format!("k{i:04}").as_bytes()).unwrap();
                assert_eq!(val, Some(format!("v{i:04}").into_bytes()), "key k{i:04}");
            }
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sequence_advances() {
        let dir = temp_dir("sequence");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();

        assert_eq!(engine.sequence(), 0);
        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        assert_eq!(engine.sequence(), 1);
        engine.delete(b"b".to_vec()).unwrap();
        assert_eq!(engine.sequence(), 2);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn manual_flush_and_compact() {
        let dir = temp_dir("manual_compact");
        let config = StorageConfig {
            memtable_size: 1024 * 1024,
            compaction: CompactionConfig {
                level_threshold: 2,
                max_levels: 3,
                block_size: 128,
            },
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        // Write and manually flush twice to get 2 L0 tables.
        for i in 0u32..5 {
            engine
                .put(format!("a{i:03}").into_bytes(), b"val".to_vec())
                .unwrap();
        }
        engine.flush().unwrap();

        for i in 0u32..5 {
            engine
                .put(format!("b{i:03}").into_bytes(), b"val".to_vec())
                .unwrap();
        }
        engine.flush().unwrap();

        // L0 has 2 tables → compact merges them into L1.
        let stats = engine.compact().unwrap();
        assert_eq!(stats.levels_compacted, 1);
        assert_eq!(stats.tables_merged, 2);
        assert_eq!(stats.tables_written, 1);

        // All keys still readable from the merged L1 table.
        for i in 0u32..5 {
            assert!(engine.get(format!("a{i:03}").as_bytes()).unwrap().is_some());
            assert!(engine.get(format!("b{i:03}").as_bytes()).unwrap().is_some());
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_after_flush_still_works() {
        let dir = temp_dir("delete_after_flush");
        let config = StorageConfig {
            memtable_size: 50,
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        for i in 0u32..20 {
            engine
                .put(format!("k{i:04}").into_bytes(), b"live".to_vec())
                .unwrap();
        }

        engine.delete(b"k0005".to_vec()).unwrap();
        assert_eq!(engine.get(b"k0005").unwrap(), None);
        assert_eq!(engine.get(b"k0000").unwrap(), Some(b"live".to_vec()));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn many_writes_and_reads() {
        let dir = temp_dir("many_ops");
        let config = StorageConfig {
            memtable_size: 256,
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        let n = 200u32;
        for i in 0..n {
            engine
                .put(
                    format!("key-{i:06}").into_bytes(),
                    format!("val-{i:06}").into_bytes(),
                )
                .unwrap();
        }

        for i in 0..n {
            let val = engine.get(format!("key-{i:06}").as_bytes()).unwrap();
            assert_eq!(
                val,
                Some(format!("val-{i:06}").into_bytes()),
                "failed at i={i}"
            );
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compact_deduplicates_across_flushes() {
        let dir = temp_dir("compact_dedup");
        let config = StorageConfig {
            memtable_size: 1024 * 1024,
            compaction: CompactionConfig {
                level_threshold: 2,
                max_levels: 3,
                block_size: 128,
            },
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        // Flush 1: key=X val=old
        engine.put(b"X".to_vec(), b"old".to_vec()).unwrap();
        engine.flush().unwrap();

        // Flush 2: key=X val=new (newer table id wins)
        engine.put(b"X".to_vec(), b"new".to_vec()).unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();

        assert_eq!(engine.get(b"X").unwrap(), Some(b"new".to_vec()));

        fs::remove_dir_all(&dir).ok();
    }
}
