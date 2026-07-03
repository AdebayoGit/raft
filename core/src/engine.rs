//! StorageEngine — wires WAL, MemTable, SSTable, Compaction, and Manifest
//! into a single cohesive read/write interface.
//!
//! Write path:  put/delete → WAL append → MemTable insert → flush to SSTable if full
//! Read path:   get → MemTable → SSTables (newest first, L0 → Lmax)

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compaction::{CompactionConfig, DeviceState};
use crate::crypto::{Cipher, EncryptionKey};
use crate::manifest::{Manifest, SSTableMeta, TableId};
use crate::memtable::MemTable;
use crate::sstable::{BlockCache, SSTableError, SSTableIter, SSTableReader, SSTableWriter};
use crate::wal::{HlcTimestamp, SyncMode, Wal, WalEntry};

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

    #[error("encryption key error: {0}")]
    Encryption(String),
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
    ///
    /// Default: 0, which means "generate a random id on first open and
    /// persist it in the database directory". A fixed, shared id would
    /// collide CRDT identities across installs (counter deltas, OR-Set
    /// tags), so 0 is never used as an actual device id.
    pub device_id: u128,
    /// WAL durability policy. Default: [`SyncMode::Always`] — every
    /// acknowledged write survives power loss.
    pub wal_sync: SyncMode,
    /// WAL preallocation chunk in bytes (0 disables). Preallocating keeps
    /// per-append fsyncs from also journaling file-size updates.
    /// Default: 1 MiB.
    pub wal_preallocate: u64,
    /// Byte cap for the shared SSTable data-block cache. Total cached
    /// block bytes stay under this limit regardless of how many tables
    /// are live. Default: 8 MiB.
    pub block_cache_bytes: usize,
    /// Encryption-at-rest key (F4). When set, every WAL entry, SSTable
    /// region, and manifest record is sealed with AES-256-GCM. Key custody
    /// (Keychain, Android Keystore, …) belongs to the platform bindings.
    /// Default: `None` — files are written in plaintext.
    ///
    /// A database must always be reopened with the same key it was created
    /// with; mixing keys (or omitting one) fails with an integrity error.
    pub encryption_key: Option<EncryptionKey>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            block_size: 4096,
            compaction: CompactionConfig::default(),
            device_id: 0,
            wal_sync: SyncMode::Always,
            wal_preallocate: 1024 * 1024,
            block_cache_bytes: 8 * 1024 * 1024,
            encryption_key: None,
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

/// A live key-value pair returned by scans (tombstones excluded).
pub type ScanEntry = (Vec<u8>, Vec<u8>);

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
    /// Open `SSTableReader`s, keyed by table id. Lazily populated on first
    /// read of a table and explicitly evicted on compaction.
    ///
    /// Readers are lightweight: each holds only a file handle, bloom
    /// filter, and index block. Data-block bytes live in `block_cache`,
    /// which is byte-capped, so total memory stays bounded regardless of
    /// how many tables are live.
    reader_cache: Mutex<HashMap<TableId, Arc<SSTableReader>>>,
    /// Byte-capped LRU cache of SSTable data blocks, shared by every
    /// reader opened through `reader_for`.
    block_cache: Arc<BlockCache>,
    /// Last platform-reported device state; gates `maybe_compact`.
    device_state: DeviceState,
    /// Shared AEAD cipher when encryption at rest is enabled (F4).
    cipher: Option<Arc<Cipher>>,
}

impl StorageEngine {
    /// Open or create a database at `db_dir`.
    ///
    /// On open: replays the manifest to learn which SSTables are live,
    /// then replays the WAL to recover any unflushed memtable state.
    pub fn open(db_dir: impl AsRef<Path>, config: StorageConfig) -> Result<Self, StorageError> {
        let mut config = config;
        let db_dir = db_dir.as_ref().to_path_buf();
        fs::create_dir_all(&db_dir)?;

        // Resolve the device identity: explicit config wins, otherwise
        // load (or generate and persist) a random per-install id.
        if config.device_id == 0 {
            config.device_id = load_or_create_device_id(&db_dir)?;
        }

        // Ensure level directories exist.
        let sstables_dir = db_dir.join("sstables");
        for l in 0..config.compaction.max_levels {
            fs::create_dir_all(sstables_dir.join(format!("L{l}")))?;
        }

        // Build the shared AEAD cipher once when a key is configured.
        let cipher = config
            .encryption_key
            .as_ref()
            .map(|key| Arc::new(Cipher::new(key)));

        // Fail fast on a missing or wrong key *before* any recovery runs.
        // WAL recovery treats undecryptable frames as a torn tail and
        // truncates them — without this check a key mismatch would silently
        // destroy data instead of erroring.
        verify_key_check(&db_dir, cipher.as_deref())?;

        // Open manifest.
        let manifest_path = db_dir.join("MANIFEST");
        let manifest = Manifest::open_with_cipher(&manifest_path, cipher.clone())?;
        let version = manifest.current_version();

        // Derive next table ID from existing tables.
        let next_table_id = version.tables.keys().last().map_or(1, |max_id| max_id + 1);

        // Open WAL.
        let wal_path = db_dir.join("wal.log");
        let mut wal = Wal::open_with_cipher(&wal_path, config.wal_sync, cipher.clone())?;
        wal.set_preallocate(config.wal_preallocate);

        // Create memtable and recover the WAL. A torn tail (partial last
        // write after power loss) is expected — recover() keeps the valid
        // prefix and truncates the damage.
        let mut memtable = MemTable::new(config.memtable_size);
        let (entries, _stats) = wal.recover()?;

        // Seed the HLC from the newest recovered entry so timestamps stay
        // monotonic across restarts even if the wall clock went backwards
        // while the process was down.
        let mut hlc_physical: u64 = 0;
        let mut hlc_logical: u16 = 0;
        for entry in entries {
            if (entry.timestamp.physical, entry.timestamp.logical) > (hlc_physical, hlc_logical) {
                hlc_physical = entry.timestamp.physical;
                hlc_logical = entry.timestamp.logical;
            }
            if let Some(op) = decode_payload(&entry.payload) {
                match op {
                    WalOp::Put { key, value } => memtable.insert(key, value),
                    WalOp::Delete { key } => memtable.delete(key),
                }
            }
        }

        let block_cache = Arc::new(BlockCache::new(config.block_cache_bytes));

        // Redaction (X5): the db directory path is deployment metadata,
        // not user document data.
        tracing::info!(path = %db_dir.display(), "storage engine opened");

        Ok(Self {
            db_dir,
            config,
            wal,
            memtable,
            manifest,
            sequence: version.sequence,
            hlc_logical,
            hlc_physical,
            next_table_id,
            reader_cache: Mutex::new(HashMap::new()),
            block_cache,
            device_state: DeviceState::default(),
            cipher,
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
            // Quick key-range filter — eliminates most tables before any
            // bloom check or file access.
            if key < meta.smallest_key.as_slice() || key > meta.largest_key.as_slice() {
                continue;
            }

            let reader = match self.reader_for(meta.id, meta.level)? {
                Some(r) => r,
                None => continue, // file missing — treat as absent
            };
            if let Some(maybe_value) = reader.get(key)? {
                return match maybe_value {
                    Some(v) => Ok(Some(v)),
                    None => Ok(None), // tombstone
                };
            }
        }

        Ok(None)
    }

    /// Scan all live key-value pairs whose key starts with `prefix`,
    /// returned in ascending key order. Tombstoned keys are excluded.
    ///
    /// Merge order: SSTables oldest → newest (higher level first, then
    /// ascending id within a level), memtable last — so the newest write
    /// for each key wins, matching the `get` read path.
    ///
    /// An empty `prefix` scans the entire keyspace.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<ScanEntry>, StorageError> {
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();

        // SSTables oldest first: level descending, id ascending.
        let version = self.manifest.current_version();
        let mut tables: Vec<&SSTableMeta> = version.tables.values().collect();
        tables.sort_by(|a, b| b.level.cmp(&a.level).then_with(|| a.id.cmp(&b.id)));

        for meta in tables {
            // Skip tables whose key range cannot contain the prefix.
            if !meta.largest_key.starts_with(prefix) && meta.largest_key.as_slice() < prefix {
                continue;
            }
            let reader = match self.reader_for(meta.id, meta.level)? {
                Some(r) => r,
                None => continue, // file missing — raced with compaction
            };
            for (k, v) in reader.scan_all()? {
                if k.starts_with(prefix) {
                    merged.insert(k, v);
                }
            }
        }

        // Memtable on top — newest state, including tombstones.
        for (k, v) in self.memtable.iter() {
            if k.starts_with(prefix) {
                merged.insert(k.to_vec(), v.map(|v| v.to_vec()));
            }
        }

        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect())
    }

    /// Return a cached `SSTableReader` for the given table, opening and
    /// caching it on first use. Returns `Ok(None)` if the file is missing
    /// (e.g. raced with compaction).
    fn reader_for(
        &self,
        id: TableId,
        level: u32,
    ) -> Result<Option<Arc<SSTableReader>>, StorageError> {
        // Fast path: already cached.
        {
            let cache = self.reader_cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(r) = cache.get(&id) {
                return Ok(Some(Arc::clone(r)));
            }
        }
        // Slow path: open the file, then insert. We open outside the lock
        // to avoid blocking other readers on disk I/O. The race where two
        // threads open the same table concurrently is benign: whichever
        // entry lands in the map first wins, the other Arc is dropped.
        let path = self.sstable_path(id, level);
        if !path.exists() {
            return Ok(None);
        }
        let reader = Arc::new(SSTableReader::open_with_cache_and_cipher(
            &path,
            Arc::clone(&self.block_cache),
            id,
            self.cipher.clone(),
        )?);
        let mut cache = self.reader_cache.lock().unwrap_or_else(|e| e.into_inner());
        let entry = cache.entry(id).or_insert_with(|| Arc::clone(&reader));
        Ok(Some(Arc::clone(entry)))
    }

    /// Drop a cached reader — called when an SSTable is removed by
    /// compaction. Outstanding `Arc`s held by in-flight `get`s remain
    /// valid until those calls complete.
    fn evict_reader(&self, id: TableId) {
        self.reader_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
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

    /// Update the platform-reported device state (idle / charging /
    /// battery). Platforms should call this from their power and idle
    /// callbacks; `maybe_compact` consults the latest state.
    pub fn set_device_state(&mut self, state: DeviceState) {
        self.device_state = state;
    }

    /// The last device state reported via [`Self::set_device_state`].
    pub fn device_state(&self) -> DeviceState {
        self.device_state
    }

    /// Run a compaction pass only if the current device state allows it
    /// (idle, and not on a low battery unless charging).
    ///
    /// Returns `Ok(None)` when compaction was deferred, `Ok(Some(stats))`
    /// when a pass ran. Use [`Self::compact`] to force a pass regardless
    /// of device state.
    pub fn maybe_compact(&mut self) -> Result<Option<CompactionStats>, StorageError> {
        if !self.device_state.allows_compaction() {
            return Ok(None);
        }
        self.compact().map(Some)
    }

    /// Current DB sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// The device id stamped on every WAL entry — either the configured
    /// value or the persisted per-install id generated on first open.
    pub fn device_id(&self) -> u128 {
        self.config.device_id
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

        // Stream-merge entries from all tables (k-way merge over one
        // decoded block per table — never the full level in RAM). Tables
        // are sorted by id ascending in the manifest (oldest first); for
        // each key, the newest (highest-id) entry wins. Readers are opened
        // without the block cache so this one-shot pass doesn't pollute it.
        let readers: Vec<SSTableReader> = tables
            .iter()
            .map(|meta| {
                SSTableReader::open_with_cipher(
                    self.sstable_path(meta.id, meta.level),
                    self.cipher.clone(),
                )
            })
            .collect::<Result<_, _>>()?;

        let new_id = self.next_table_id;
        self.next_table_id += 1;

        let mut merge_state = MergeState::default();
        let merge = CompactMerge {
            iters: readers.iter().map(|r| r.iter().peekable()).collect(),
            state: &mut merge_state,
        };

        let out_path = self.sstable_path(new_id, next_level);
        let writer = SSTableWriter::new(&out_path)
            .with_block_size(self.config.block_size)
            .with_cipher(self.cipher.clone());
        let write_result = writer.write(merge);

        // A block-read failure mid-merge surfaces via the merge state,
        // not the writer. Discard the partial output file in that case.
        if let Some(err) = merge_state.error.take() {
            fs::remove_file(&out_path).ok();
            return Err(err.into());
        }
        let entry_count = write_result? as u64;

        let smallest_key = merge_state.first_key.take().unwrap_or_default();
        let largest_key = merge_state.last_key.take().unwrap_or_default();

        // Make the new file's directory entry durable before the manifest
        // references it (the file itself is fsynced by the writer).
        sync_dir(out_path.parent().unwrap_or(Path::new(".")))?;

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

        // Remove old tables from manifest, evict their cached readers,
        // and delete files.
        for meta in &tables {
            self.manifest.remove_sstable(meta.id)?;
            self.evict_reader(meta.id);
            self.block_cache.evict_table(meta.id);
            let old_path = self.sstable_path(meta.id, meta.level);
            fs::remove_file(&old_path).ok();
            stats.tables_deleted += 1;
        }
        stats.tables_merged += tables.len();
        stats.levels_compacted += 1;

        // Redaction (X5): counts and sizes only — never keys or values.
        tracing::info!(
            level,
            next_level,
            tables_merged = tables.len(),
            entries = entry_count,
            bytes = file_size,
            "compaction pass complete"
        );

        Ok(())
    }

    /// Flush the current memtable to a new L0 SSTable.
    fn flush_memtable(&mut self) -> Result<(), StorageError> {
        let table_id = self.next_table_id;
        self.next_table_id += 1;

        // Swap in a fresh memtable.
        let old = std::mem::replace(&mut self.memtable, MemTable::new(self.config.memtable_size));

        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = old.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }

        let smallest_key = entries.first().map(|(k, _)| k.clone()).unwrap_or_default();
        let largest_key = entries.last().map(|(k, _)| k.clone()).unwrap_or_default();
        let entry_count = entries.len() as u64;

        let path = self.sstable_path(table_id, 0);
        let writer = SSTableWriter::new(&path)
            .with_block_size(self.config.block_size)
            .with_cipher(self.cipher.clone());
        writer.write(entries.into_iter())?;

        // Crash-ordering: the SSTable file is fsynced by the writer; also
        // fsync its directory so the new directory entry is durable before
        // the manifest references it.
        sync_dir(path.parent().unwrap_or(Path::new(".")))?;

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

        // Truncate WAL in place — memtable data is now durable in the
        // SSTable and registered in the (fsynced) manifest.
        self.wal.reset()?;

        // Redaction (X5): counts and sizes only — never keys or values.
        tracing::debug!(
            table_id,
            entries = entry_count,
            bytes = file_size,
            "memtable flushed to L0"
        );

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
    ///
    /// Timestamps are strictly monotonic even when the wall clock stalls
    /// or moves backwards: the physical component never regresses, and a
    /// logical-counter overflow advances the physical component by 1 ms
    /// instead of wrapping back to zero.
    fn advance_hlc(&mut self) -> HlcTimestamp {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if now_ms > self.hlc_physical {
            self.hlc_physical = now_ms;
            self.hlc_logical = 0;
        } else if self.hlc_logical == u16::MAX {
            // Logical counter exhausted within this millisecond — borrow
            // a millisecond from the physical component rather than
            // wrapping, which would make timestamps travel backwards.
            self.hlc_physical += 1;
            self.hlc_logical = 0;
        } else {
            self.hlc_logical += 1;
        }

        HlcTimestamp::new(self.hlc_physical, self.hlc_logical)
    }
}

/// Known plaintext sealed into the `KEYCHECK` sentinel file on first open
/// of an encrypted database.
const KEY_CHECK_PLAINTEXT: &[u8] = b"raft-db keycheck v1";

/// Detect encryption-key mismatches before any recovery runs.
///
/// On first open with a key, seals a known plaintext into `KEYCHECK`.
/// On subsequent opens the sentinel must decrypt back to that plaintext:
/// - key present, sentinel missing → created (first encrypted open)
/// - key present, sentinel fails to open → wrong key
/// - key absent, sentinel present → database requires its key
fn verify_key_check(db_dir: &Path, cipher: Option<&Cipher>) -> Result<(), StorageError> {
    let path = db_dir.join("KEYCHECK");
    match (cipher, path.exists()) {
        (None, false) => Ok(()),
        (None, true) => Err(StorageError::Encryption(
            "database is encrypted but no encryption_key was provided".to_string(),
        )),
        (Some(cipher), true) => {
            let sealed = fs::read(&path)?;
            match cipher.open(&sealed) {
                Ok(plaintext) if plaintext == KEY_CHECK_PLAINTEXT => Ok(()),
                _ => Err(StorageError::Encryption(
                    "encryption key does not match this database".to_string(),
                )),
            }
        }
        (Some(cipher), false) => {
            // A pre-existing database without a sentinel is plaintext —
            // opening it with a key is a configuration error, not a reason
            // to plant a sentinel in an unencrypted directory.
            if db_dir.join("MANIFEST").exists() || db_dir.join("wal.log").exists() {
                return Err(StorageError::Encryption(
                    "database is not encrypted but an encryption_key was provided".to_string(),
                ));
            }
            let sealed = cipher
                .seal(KEY_CHECK_PLAINTEXT)
                .map_err(|e| StorageError::Encryption(e.to_string()))?;
            // Persist durably: file contents, then the directory entry.
            let mut file = fs::File::create(&path)?;
            std::io::Write::write_all(&mut file, &sealed)?;
            file.sync_all()?;
            sync_dir(db_dir)?;
            Ok(())
        }
    }
}

/// Load the persisted 128-bit device id from `DEVICE_ID`, or generate a
/// random one and persist it durably on first open.
///
/// The id is never 0 — that value is reserved to mean "not configured".
fn load_or_create_device_id(db_dir: &Path) -> Result<u128, StorageError> {
    let path = db_dir.join("DEVICE_ID");

    if let Ok(bytes) = fs::read(&path) {
        if bytes.len() == 16 {
            let id = u128::from_be_bytes(bytes.try_into().expect("length checked"));
            if id != 0 {
                return Ok(id);
            }
        }
        // Malformed or zero — regenerate below.
    }

    let mut buf = [0u8; 16];
    loop {
        getrandom::fill(&mut buf).map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "failed to generate device id: {e}"
            )))
        })?;
        if buf != [0u8; 16] {
            break;
        }
    }

    // Persist durably: file contents, then the directory entry.
    let mut file = fs::File::create(&path)?;
    std::io::Write::write_all(&mut file, &buf)?;
    file.sync_all()?;
    sync_dir(db_dir)?;

    Ok(u128::from_be_bytes(buf))
}

/// fsync a directory so that recently created/removed directory entries
/// are durable. No-op on platforms where directories cannot be synced.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        // Windows does not support fsync on directory handles via
        // std::fs — metadata durability is handled by the filesystem.
        let _ = dir;
    }
    Ok(())
}

/// Out-of-band results of a [`CompactMerge`] pass: the writer consumes
/// the iterator by value, so errors and key-range bookkeeping are
/// reported through this shared state instead of the iterator itself.
#[derive(Default)]
struct MergeState {
    /// First block-read/decode failure, if any. Once set, the merge
    /// yields no further entries.
    error: Option<SSTableError>,
    /// Smallest key yielded so far.
    first_key: Option<Vec<u8>>,
    /// Largest key yielded so far.
    last_key: Option<Vec<u8>>,
}

/// Streaming k-way merge over per-table iterators for compaction.
///
/// `iters` must be ordered oldest-to-newest; when several tables contain
/// the same key, the newest table's entry (including tombstones) wins.
/// Peak memory is one decoded block per input table.
struct CompactMerge<'r, 's> {
    iters: Vec<std::iter::Peekable<SSTableIter<'r>>>,
    state: &'s mut MergeState,
}

impl Iterator for CompactMerge<'_, '_> {
    type Item = (Vec<u8>, Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.state.error.is_some() {
            return None;
        }

        // Find the smallest key among the iterator heads.
        let mut min_key: Option<Vec<u8>> = None;
        for it in self.iters.iter_mut() {
            match it.peek() {
                Some(Ok((k, _))) => {
                    if min_key.as_ref().is_none_or(|m| k < m) {
                        min_key = Some(k.clone());
                    }
                }
                Some(Err(_)) => {
                    if let Some(Err(e)) = it.next() {
                        self.state.error = Some(e);
                    }
                    return None;
                }
                None => {}
            }
        }
        let min_key = min_key?;

        // Consume that key from every table holding it; iterating
        // oldest-to-newest means the last value taken is the newest.
        let mut winner: Option<Option<Vec<u8>>> = None;
        for it in self.iters.iter_mut() {
            let head_matches = matches!(it.peek(), Some(Ok((k, _))) if *k == min_key);
            if head_matches {
                if let Some(Ok((_, v))) = it.next() {
                    winner = Some(v);
                }
            }
        }

        self.state.first_key.get_or_insert_with(|| min_key.clone());
        self.state.last_key = Some(min_key.clone());
        winner.map(|v| (min_key, v))
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
        let dir = std::env::temp_dir().join("raft_db_engine_tests").join(name);
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
            wal_sync: SyncMode::Always,
            wal_preallocate: 1024 * 1024,
            block_cache_bytes: 4096,
            encryption_key: None,
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
                .put(
                    format!("k{i:04}").into_bytes(),
                    format!("v{i:04}").into_bytes(),
                )
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
                    .put(
                        format!("k{i:04}").into_bytes(),
                        format!("v{i:04}").into_bytes(),
                    )
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
    fn reopen_recovers_from_torn_wal_tail() {
        let dir = temp_dir("torn_wal_tail");
        let config = default_config();

        {
            let mut engine = StorageEngine::open(&dir, config.clone()).unwrap();
            engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
            engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        }

        // Simulate a torn write: append garbage that looks like a partial
        // entry to the WAL tail.
        let wal_path = dir.join("wal.log");
        let mut data = fs::read(&wal_path).unwrap();
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05]);
        fs::write(&wal_path, &data).unwrap();

        // Reopen must succeed and recover both committed writes.
        let engine = StorageEngine::open(&dir, config).unwrap();
        assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn device_id_generated_and_persisted_when_unset() {
        let dir = temp_dir("device_id_gen");
        let config = StorageConfig {
            device_id: 0, // ask for auto-generation
            ..default_config()
        };

        let first = {
            let engine = StorageEngine::open(&dir, config.clone()).unwrap();
            let id = engine.device_id();
            assert_ne!(id, 0, "generated device id must be nonzero");
            id
        };

        assert!(dir.join("DEVICE_ID").exists());

        // Reopen: same id.
        let engine = StorageEngine::open(&dir, config).unwrap();
        assert_eq!(engine.device_id(), first);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explicit_device_id_is_respected() {
        let dir = temp_dir("device_id_explicit");
        let engine = StorageEngine::open(&dir, default_config()).unwrap();
        assert_eq!(engine.device_id(), 0xDEAD);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn distinct_databases_get_distinct_device_ids() {
        let dir_a = temp_dir("device_id_a");
        let dir_b = temp_dir("device_id_b");
        let config = StorageConfig {
            device_id: 0,
            ..default_config()
        };

        let a = StorageEngine::open(&dir_a, config.clone()).unwrap();
        let b = StorageEngine::open(&dir_b, config).unwrap();
        assert_ne!(a.device_id(), b.device_id());

        fs::remove_dir_all(&dir_a).ok();
        fs::remove_dir_all(&dir_b).ok();
    }

    #[test]
    fn hlc_logical_overflow_advances_physical() {
        let dir = temp_dir("hlc_overflow");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();

        // Pin the clock in the future so now_ms <= hlc_physical, forcing
        // the logical-increment path; exhaust the logical counter.
        engine.hlc_physical = u64::MAX - 10;
        engine.hlc_logical = u16::MAX;

        let ts = engine.advance_hlc();
        assert_eq!(ts.physical, u64::MAX - 9, "overflow must borrow 1 ms");
        assert_eq!(ts.logical, 0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn hlc_is_strictly_monotonic_under_stalled_clock() {
        let dir = temp_dir("hlc_monotonic");
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();
        engine.hlc_physical = u64::MAX - 1_000_000; // clock appears stalled

        let mut last = engine.advance_hlc();
        for _ in 0..200_000 {
            let ts = engine.advance_hlc();
            assert!(
                (ts.physical, ts.logical) > (last.physical, last.logical),
                "HLC went backwards: {ts:?} after {last:?}"
            );
            last = ts;
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn hlc_seeded_from_wal_on_reopen() {
        let dir = temp_dir("hlc_reseed");
        let future = u64::MAX / 2; // far beyond any real wall clock

        {
            let mut engine = StorageEngine::open(&dir, default_config()).unwrap();
            engine.hlc_physical = future;
            engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        }

        // Reopen: the recovered WAL entry carries the future timestamp, so
        // the HLC must resume at or beyond it — not restart at wall clock.
        let mut engine = StorageEngine::open(&dir, default_config()).unwrap();
        assert!(engine.hlc_physical >= future);
        let ts = engine.advance_hlc();
        assert!(ts.physical >= future);

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

    #[test]
    fn streaming_compaction_merges_overlapping_multi_block_tables() {
        let dir = temp_dir("compact_streaming");
        let config = StorageConfig {
            memtable_size: 1024 * 1024,
            compaction: CompactionConfig {
                level_threshold: 3,
                max_levels: 3,
                block_size: 128,
            },
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        // Three overlapping flushes, each spanning many blocks.
        for round in 0..3 {
            for i in (round..200).step_by(3) {
                engine
                    .put(
                        format!("key-{i:05}").into_bytes(),
                        format!("r{round}-value-{i:05}").into_bytes(),
                    )
                    .unwrap();
            }
            // Overlap: every round rewrites key-00000..key-00010.
            for i in 0..10 {
                engine
                    .put(
                        format!("key-{i:05}").into_bytes(),
                        format!("r{round}-overlap-{i:05}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        // Newest flush deletes one key.
        engine.delete(b"key-00099".to_vec()).unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap();
        assert!(stats.tables_merged >= 3);

        // Overlapping keys resolve to the newest round.
        for i in 0..10 {
            assert_eq!(
                engine.get(format!("key-{i:05}").as_bytes()).unwrap(),
                Some(format!("r2-overlap-{i:05}").into_bytes()),
                "overlap key {i}"
            );
        }
        // Deleted key stays deleted after the merge.
        assert_eq!(engine.get(b"key-00099").unwrap(), None);
        // A non-overlapping key from each round survives.
        assert_eq!(
            engine.get(b"key-00033").unwrap(),
            Some(b"r0-value-00033".to_vec())
        );
        assert_eq!(
            engine.get(b"key-00034").unwrap(),
            Some(b"r1-value-00034".to_vec())
        );
        assert_eq!(
            engine.get(b"key-00035").unwrap(),
            Some(b"r2-value-00035".to_vec())
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn block_cache_stays_bounded_across_many_tables() {
        let dir = temp_dir("block_cache_bounded");
        // Tiny cache so eviction churns constantly across reads.
        let config = StorageConfig {
            block_cache_bytes: 512,
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        // Write enough to flush several SSTables (memtable_size = 4096).
        for i in 0..300 {
            engine
                .put(
                    format!("key-{i:06}").into_bytes(),
                    format!("value-data-{i:06}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush().unwrap();

        // Every key readable, cache never exceeds its cap.
        for i in 0..300 {
            let val = engine.get(format!("key-{i:06}").as_bytes()).unwrap();
            assert_eq!(val, Some(format!("value-data-{i:06}").into_bytes()));
            assert!(engine.block_cache.current_bytes() <= engine.block_cache.capacity_bytes());
        }
        assert!(engine.block_cache.hits() + engine.block_cache.misses() > 0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_evicts_dead_tables_from_block_cache() {
        let dir = temp_dir("block_cache_compact_evict");
        let config = StorageConfig {
            memtable_size: 1024 * 1024,
            block_cache_bytes: 1024 * 1024,
            compaction: CompactionConfig {
                level_threshold: 2,
                max_levels: 3,
                block_size: 128,
            },
            ..default_config()
        };
        let mut engine = StorageEngine::open(&dir, config).unwrap();

        for round in 0..2 {
            for i in 0..50 {
                engine
                    .put(
                        format!("k{i:04}").into_bytes(),
                        format!("v{round}-{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        // Populate the cache from the L0 tables.
        for i in 0..50 {
            engine.get(format!("k{i:04}").as_bytes()).unwrap();
        }
        assert!(engine.block_cache.current_bytes() > 0);

        let stats = engine.compact().unwrap();
        assert!(stats.tables_deleted > 0);

        // Reads after compaction return the newest values.
        for i in 0..50 {
            assert_eq!(
                engine.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(format!("v1-{i:04}").into_bytes())
            );
        }

        fs::remove_dir_all(&dir).ok();
    }

    /// Build an engine with enough L0 tables to make a compaction pass
    /// do real work (level_threshold = 2).
    fn engine_ready_for_compaction(dir: &Path) -> StorageEngine {
        let config = StorageConfig {
            memtable_size: 1024 * 1024,
            compaction: CompactionConfig {
                level_threshold: 2,
                max_levels: 3,
                block_size: 128,
            },
            ..default_config()
        };
        let mut engine = StorageEngine::open(dir, config).unwrap();
        for round in 0..2 {
            for i in 0..20 {
                engine
                    .put(
                        format!("k{i:04}").into_bytes(),
                        format!("v{round}-{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        engine
    }

    #[test]
    fn maybe_compact_defers_when_device_busy() {
        let dir = temp_dir("maybe_compact_busy");
        let mut engine = engine_ready_for_compaction(&dir);

        // Default state is not idle — compaction must be deferred.
        assert_eq!(engine.device_state(), DeviceState::default());
        let l0_before = engine.manifest.tables_at_level(0).len();
        assert!(l0_before >= 2);

        let result = engine.maybe_compact().unwrap();
        assert_eq!(result, None);
        assert_eq!(engine.manifest.tables_at_level(0).len(), l0_before);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn maybe_compact_runs_on_idle_signal() {
        let dir = temp_dir("maybe_compact_idle");
        let mut engine = engine_ready_for_compaction(&dir);

        // Simulated platform idle signal.
        engine.set_device_state(DeviceState {
            idle: true,
            charging: false,
            battery_low: false,
        });

        let stats = engine.maybe_compact().unwrap().expect("should compact");
        assert!(stats.tables_merged >= 2);
        assert!(engine.manifest.tables_at_level(0).is_empty());

        // Data intact after the gated pass.
        for i in 0..20 {
            assert_eq!(
                engine.get(format!("k{i:04}").as_bytes()).unwrap(),
                Some(format!("v1-{i:04}").into_bytes())
            );
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn maybe_compact_defers_on_low_battery_unless_charging() {
        let dir = temp_dir("maybe_compact_battery");
        let mut engine = engine_ready_for_compaction(&dir);

        // Idle but low battery and unplugged — deferred.
        engine.set_device_state(DeviceState {
            idle: true,
            charging: false,
            battery_low: true,
        });
        assert_eq!(engine.maybe_compact().unwrap(), None);

        // Plugging in unblocks the pass.
        engine.set_device_state(DeviceState {
            idle: true,
            charging: true,
            battery_low: true,
        });
        assert!(engine.maybe_compact().unwrap().is_some());

        fs::remove_dir_all(&dir).ok();
    }
}
