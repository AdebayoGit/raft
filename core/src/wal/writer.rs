use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::entry::{WalEntry, MAX_PAYLOAD_LEN};
use super::error::WalError;
use crate::crypto::{Cipher, SEAL_OVERHEAD};

/// Durability policy applied after each WAL append.
///
/// Controls when the WAL fsyncs to durable storage. `Always` is the safe
/// default: a successful `append` guarantees the entry survives power loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// fsync after every append. Maximum durability (default).
    #[default]
    Always,
    /// fsync after every N appends. Bounded data-loss window of at most
    /// N-1 entries on power loss. `EveryN(0)` behaves like `EveryN(1)`.
    EveryN(u32),
    /// Never fsync automatically — caller must invoke [`Wal::sync`].
    /// Data-loss window is unbounded on power loss.
    Off,
}

/// Write-ahead log backed by a single log file with positioned writes.
///
/// All mutations flow through the WAL before reaching the memtable.
/// On recovery, `recover()` reads every entry back in order.
///
/// The file may be preallocated in chunks (see [`Wal::set_preallocate`])
/// so that appends don't force a file-size metadata update on every fsync.
/// The preallocated tail is zero-filled; an all-zero region is never a
/// valid entry (its checksum can't be zero), so readers treat it as EOF.
pub struct Wal {
    path: PathBuf,
    file: File,
    /// Logical end of valid data — next append writes here.
    write_pos: u64,
    /// Physical file size (>= write_pos when preallocated).
    capacity: u64,
    sync_mode: SyncMode,
    appends_since_sync: u32,
    /// Preallocation chunk size in bytes. 0 disables preallocation.
    preallocate: u64,
    /// When set, entries are sealed with AES-256-GCM before hitting disk
    /// (F4 — encryption at rest). Framing: `[len: u32][nonce||ct||tag]`.
    cipher: Option<Arc<Cipher>>,
}

/// Maximum accepted sealed-frame length when decoding an encrypted WAL.
/// A frame wraps one encoded entry (header + payload + crc), so this is
/// the payload cap plus fixed entry framing plus the AEAD envelope.
const MAX_FRAME_LEN: usize = MAX_PAYLOAD_LEN + 64 + SEAL_OVERHEAD;

/// Returns true if every byte in the slice is zero (preallocated tail).
fn all_zeros(data: &[u8]) -> bool {
    data.iter().all(|&b| b == 0)
}

/// Decode the next entry starting at `pos`, in either plaintext or
/// encrypted framing depending on `cipher`.
///
/// Returns `Ok(Some((entry, next_pos)))`, `Ok(None)` at EOF or a
/// zero-filled preallocated tail, or an error for damaged bytes.
fn decode_next(
    cipher: Option<&Cipher>,
    data: &[u8],
    pos: usize,
) -> Result<Option<(WalEntry, usize)>, WalError> {
    if pos >= data.len() || all_zeros(&data[pos..]) {
        return Ok(None);
    }

    let Some(cipher) = cipher else {
        let mut cursor = &data[pos..];
        return Ok(WalEntry::decode(&mut cursor, pos as u64)?
            .map(|entry| (entry, data.len() - cursor.len())));
    };

    // Encrypted framing: [frame_len: u32][nonce||ciphertext||tag].
    let offset = pos as u64;
    let rest = &data[pos..];
    if rest.len() < 4 {
        return Err(WalError::IncompleteEntry {
            offset,
            needed: 4,
            available: rest.len(),
        });
    }
    let frame_len = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
    if !(SEAL_OVERHEAD..=MAX_FRAME_LEN).contains(&frame_len) {
        return Err(WalError::PayloadTooLarge {
            offset,
            len: frame_len,
            max: MAX_FRAME_LEN,
        });
    }
    if rest.len() < 4 + frame_len {
        return Err(WalError::IncompleteEntry {
            offset,
            needed: 4 + frame_len,
            available: rest.len(),
        });
    }

    let plaintext = cipher
        .open(&rest[4..4 + frame_len])
        .map_err(|_| WalError::BadCiphertext { offset })?;
    let mut cursor: &[u8] = &plaintext;
    let entry = WalEntry::decode(&mut cursor, offset)?.ok_or(WalError::IncompleteEntry {
        offset,
        needed: 1,
        available: 0,
    })?;
    if !cursor.is_empty() {
        // A frame must contain exactly one entry.
        return Err(WalError::BadCiphertext { offset });
    }
    Ok(Some((entry, pos + 4 + frame_len)))
}

/// Length of the valid entry prefix in `data`.
///
/// Scans entries until EOF, a zero-filled tail, or the first damaged entry.
fn valid_prefix_len(cipher: Option<&Cipher>, data: &[u8]) -> usize {
    let mut pos = 0usize;
    while let Ok(Some((_, next))) = decode_next(cipher, data, pos) {
        pos = next;
    }
    pos
}

impl Wal {
    /// Open (or create) a WAL file at `path` with the default
    /// [`SyncMode::Always`] durability policy.
    ///
    /// Existing data is preserved; new appends continue after the last
    /// valid entry.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        Self::open_with_mode(path, SyncMode::Always)
    }

    /// Open (or create) a WAL file at `path` with an explicit sync mode.
    pub fn open_with_mode(path: impl AsRef<Path>, sync_mode: SyncMode) -> Result<Self, WalError> {
        Self::open_with_cipher(path, sync_mode, None)
    }

    /// Open (or create) a WAL file with an explicit sync mode and optional
    /// encryption cipher (F4). When a cipher is supplied every appended
    /// entry is sealed with AES-256-GCM, and existing data is expected to
    /// be encrypted under the same key.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        sync_mode: SyncMode,
        cipher: Option<Arc<Cipher>>,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let write_pos = valid_prefix_len(cipher.as_deref(), &data) as u64;
        let capacity = data.len() as u64;

        Ok(Self {
            path,
            file,
            write_pos,
            capacity,
            sync_mode,
            appends_since_sync: 0,
            preallocate: 0,
            cipher,
        })
    }

    /// Set the preallocation chunk size in bytes (0 disables).
    ///
    /// When enabled, the file is grown in `bytes`-sized chunks ahead of the
    /// write position so that per-append fsyncs don't also journal a file
    /// size change.
    pub fn set_preallocate(&mut self, bytes: u64) {
        self.preallocate = bytes;
    }

    /// Encode one entry into its on-disk representation: raw entry bytes
    /// in plaintext mode, `[len: u32][nonce||ct||tag]` framing when a
    /// cipher is set.
    fn encode_for_disk(&self, entry: &WalEntry) -> Result<Vec<u8>, WalError> {
        match &self.cipher {
            None => Ok(entry.encode_to_vec()),
            Some(cipher) => {
                let sealed = cipher
                    .seal(&entry.encode_to_vec())
                    .map_err(|e| WalError::Encryption(e.to_string()))?;
                let mut framed = Vec::with_capacity(4 + sealed.len());
                framed.extend_from_slice(&(sealed.len() as u32).to_be_bytes());
                framed.extend_from_slice(&sealed);
                Ok(framed)
            }
        }
    }

    /// Write pre-encoded bytes at the logical end of the log, then apply
    /// the durability policy counting `appends` logical entries.
    fn write_at_end(&mut self, encoded: &[u8], appends: u32) -> Result<(), WalError> {
        let end = self.write_pos + encoded.len() as u64;

        // Grow the file ahead of the write position in chunks.
        if self.preallocate > 0 && end > self.capacity {
            let new_cap = end.max(self.capacity + self.preallocate);
            self.file.set_len(new_cap)?;
            self.capacity = new_cap;
        }

        self.file.seek(SeekFrom::Start(self.write_pos))?;
        self.file.write_all(encoded)?;
        self.write_pos = end;
        self.capacity = self.capacity.max(end);
        self.appends_since_sync = self.appends_since_sync.saturating_add(appends);

        match self.sync_mode {
            SyncMode::Always => self.sync()?,
            SyncMode::EveryN(n) => {
                if self.appends_since_sync >= n.max(1) {
                    self.sync()?;
                }
            }
            SyncMode::Off => {}
        }
        Ok(())
    }

    /// Append a single entry to the log, then apply the durability policy.
    ///
    /// With [`SyncMode::Always`] (the default) the entry is fsynced to
    /// durable storage before this returns.
    pub fn append(&mut self, entry: &WalEntry) -> Result<(), WalError> {
        let encoded = self.encode_for_disk(entry)?;
        self.write_at_end(&encoded, 1)
    }

    /// Append many entries with a single positioned write and a single
    /// durability decision (S7f — group commit).
    ///
    /// Under [`SyncMode::Always`] the whole batch is made durable by
    /// *one* fsync instead of one per entry, amortising the dominant
    /// cost of the write path. The on-disk encoding is identical to
    /// per-entry appends, so replay and recovery are unaffected. Under
    /// [`SyncMode::EveryN`] the batch counts as `entries.len()` appends.
    ///
    /// Note: if the process crashes mid-batch, the torn tail is
    /// truncated by [`Wal::recover`] exactly as with single appends —
    /// callers must not report success until this returns.
    pub fn append_batch(&mut self, entries: &[WalEntry]) -> Result<(), WalError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut encoded = Vec::new();
        for entry in entries {
            encoded.extend_from_slice(&self.encode_for_disk(entry)?);
        }
        self.write_at_end(&encoded, entries.len() as u32)
    }

    /// Replay the entire log, yielding entries in append order.
    ///
    /// Opens a fresh read handle so it can be called while the writer is live.
    ///
    /// Strict: iteration stops at the first decode error and yields it.
    /// For crash recovery use [`Wal::recover`], which treats a torn tail
    /// as expected and truncates it.
    pub fn replay(&self) -> Result<WalIterator, WalError> {
        let mut file = File::open(&self.path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(WalIterator {
            data,
            pos: 0,
            cipher: self.cipher.clone(),
        })
    }

    /// Recover entries after a crash.
    ///
    /// Decodes the log from the start and returns every entry up to the
    /// first corruption (torn write, bad checksum, truncated record). A
    /// damaged tail is *expected* after power loss — the file is truncated
    /// back to the last valid entry so subsequent appends produce a clean
    /// log, and recovery reports what happened via [`RecoveryStats`].
    ///
    /// Only I/O errors are fatal.
    pub fn recover(&mut self) -> Result<(Vec<WalEntry>, RecoveryStats), WalError> {
        let mut file = File::open(&self.path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut entries = Vec::new();
        let mut pos = 0usize;
        let mut truncated_at = None;

        loop {
            match decode_next(self.cipher.as_deref(), &data, pos) {
                Ok(Some((entry, next))) => {
                    entries.push(entry);
                    pos = next;
                }
                Ok(None) => break,
                Err(WalError::Io(e)) => return Err(WalError::Io(e)),
                Err(_) => {
                    // Torn or corrupt tail — keep the valid prefix only.
                    // Entries beyond the damage cannot be trusted because
                    // framing is lost. (With encryption, an AEAD auth
                    // failure lands here too: a torn sealed frame cannot
                    // authenticate.)
                    truncated_at = Some(pos as u64);
                    break;
                }
            }
        }

        if let Some(offset) = truncated_at {
            // Truncate the file to the valid prefix and fsync so the
            // damaged bytes can never resurface.
            self.file.set_len(pos as u64)?;
            self.file.sync_all()?;
            self.capacity = pos as u64;
            tracing::warn!(
                offset,
                entries_recovered = entries.len(),
                "wal tail truncated after corruption"
            );
        } else {
            tracing::debug!(entries_recovered = entries.len(), "wal recovery complete");
        }
        self.write_pos = pos as u64;

        let stats = RecoveryStats {
            entries_recovered: entries.len(),
            truncated_at,
        };
        Ok((entries, stats))
    }

    /// Sync the underlying file to durable storage.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.sync_all()?;
        self.appends_since_sync = 0;
        Ok(())
    }

    /// The durability policy currently in effect.
    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    /// Truncate the log to zero length in place and fsync.
    ///
    /// Used after a memtable flush once its contents are durable in an
    /// SSTable. Keeps the same inode (no delete/recreate window where a
    /// crash could leave no WAL at all).
    pub fn reset(&mut self) -> Result<(), WalError> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        self.write_pos = 0;
        self.capacity = 0;
        self.appends_since_sync = 0;
        Ok(())
    }
}

/// Outcome of a [`Wal::recover`] pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Number of valid entries recovered from the log.
    pub entries_recovered: usize,
    /// Byte offset the log was truncated back to, if a damaged tail was
    /// found. `None` means the log was fully intact.
    pub truncated_at: Option<u64>,
}

/// Iterator over WAL entries read from a snapshot of the log file.
pub struct WalIterator {
    data: Vec<u8>,
    pos: usize,
    cipher: Option<Arc<Cipher>>,
}

impl Iterator for WalIterator {
    type Item = Result<WalEntry, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        match decode_next(self.cipher.as_deref(), &self.data, self.pos) {
            Ok(Some((entry, next))) => {
                self.pos = next;
                Some(Ok(entry))
            }
            Ok(None) => None,
            Err(e) => {
                // Advance past remaining data to stop iteration after error.
                self.pos = self.data.len();
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::entry::HlcTimestamp;
    use std::fs;

    fn temp_wal_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("raft_db_tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{name}.wal"))
    }

    fn make_entry(physical: u64, logical: u16, payload: &[u8]) -> WalEntry {
        WalEntry::new(
            HlcTimestamp::new(physical, logical),
            0xAAAA_BBBB_CCCC_DDDD_1111_2222_3333_4444u128,
            payload.to_vec(),
        )
    }

    #[test]
    fn open_creates_file() {
        let path = temp_wal_path("open_creates");
        let _ = fs::remove_file(&path);

        let _wal = Wal::open(&path).expect("should open");
        assert!(path.exists());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_and_replay_single_entry() {
        let path = temp_wal_path("single_entry");
        let _ = fs::remove_file(&path);

        let entry = make_entry(1000, 0, b"first");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&entry).unwrap();
        }

        let wal = Wal::open(&path).unwrap();
        let entries: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_and_replay_multiple_entries() {
        let path = temp_wal_path("multiple_entries");
        let _ = fs::remove_file(&path);

        let entries_in: Vec<WalEntry> = (0..100)
            .map(|i| make_entry(1000 + i, i as u16, format!("payload-{i}").as_bytes()))
            .collect();

        {
            let mut wal = Wal::open(&path).unwrap();
            for entry in &entries_in {
                wal.append(entry).unwrap();
            }
        }

        let wal = Wal::open(&path).unwrap();
        let entries_out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entries_out, entries_in);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_empty_file_yields_nothing() {
        let path = temp_wal_path("empty_replay");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        let entries: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(entries.is_empty());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_detects_corruption() {
        let path = temp_wal_path("corruption");
        let _ = fs::remove_file(&path);

        let entry = make_entry(500, 1, b"important data");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&entry).unwrap();
        }

        // Corrupt a byte in the middle of the file.
        let mut data = fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let wal = Wal::open(&path).unwrap();
        let results: Vec<Result<WalEntry, WalError>> = wal.replay().unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_persists_across_reopen() {
        let path = temp_wal_path("persist_reopen");
        let _ = fs::remove_file(&path);

        let e1 = make_entry(1, 0, b"batch-1");
        let e2 = make_entry(2, 0, b"batch-2");

        // First session: write e1.
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&e1).unwrap();
        }

        // Second session: write e2.
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&e2).unwrap();
        }

        // Third session: replay both.
        let wal = Wal::open(&path).unwrap();
        let entries: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entries, vec![e1, e2]);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn default_sync_mode_is_always() {
        assert_eq!(SyncMode::default(), SyncMode::Always);

        let path = temp_wal_path("default_mode");
        let _ = fs::remove_file(&path);
        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.sync_mode(), SyncMode::Always);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn every_n_mode_syncs_on_interval() {
        let path = temp_wal_path("every_n_mode");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open_with_mode(&path, SyncMode::EveryN(3)).unwrap();
        for i in 0..2 {
            wal.append(&make_entry(i, 0, b"x")).unwrap();
        }
        assert_eq!(wal.appends_since_sync, 2);
        wal.append(&make_entry(2, 0, b"x")).unwrap();
        assert_eq!(wal.appends_since_sync, 0, "third append should sync");

        fs::remove_file(&path).ok();
    }

    #[test]
    fn every_zero_behaves_like_every_one() {
        let path = temp_wal_path("every_zero");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open_with_mode(&path, SyncMode::EveryN(0)).unwrap();
        wal.append(&make_entry(1, 0, b"x")).unwrap();
        assert_eq!(wal.appends_since_sync, 0);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn off_mode_never_auto_syncs() {
        let path = temp_wal_path("off_mode");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open_with_mode(&path, SyncMode::Off).unwrap();
        for i in 0..10 {
            wal.append(&make_entry(i, 0, b"x")).unwrap();
        }
        assert_eq!(wal.appends_since_sync, 10);
        wal.sync().unwrap();
        assert_eq!(wal.appends_since_sync, 0);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_batch_replays_in_order() {
        let path = temp_wal_path("batch_replay");
        let _ = fs::remove_file(&path);

        let batch: Vec<WalEntry> = (0..10)
            .map(|i| make_entry(100 + i, i as u16, format!("batched-{i}").as_bytes()))
            .collect();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&make_entry(1, 0, b"before")).unwrap();
            wal.append_batch(&batch).unwrap();
            wal.append(&make_entry(999, 0, b"after")).unwrap();
        }

        let wal = Wal::open(&path).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(out.len(), 12);
        assert_eq!(out[0].payload, b"before");
        assert_eq!(&out[1..11], batch.as_slice());
        assert_eq!(out[11].payload, b"after");

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_batch_empty_is_noop() {
        let path = temp_wal_path("batch_empty");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open_with_mode(&path, SyncMode::Off).unwrap();
        wal.append_batch(&[]).unwrap();
        assert_eq!(wal.appends_since_sync, 0);
        assert!(wal.replay().unwrap().next().is_none());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_batch_counts_entries_for_every_n() {
        let path = temp_wal_path("batch_every_n");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open_with_mode(&path, SyncMode::EveryN(5)).unwrap();
        let batch: Vec<WalEntry> = (0..3).map(|i| make_entry(i, 0, b"x")).collect();

        wal.append_batch(&batch).unwrap();
        assert_eq!(wal.appends_since_sync, 3, "batch counts as 3 appends");

        wal.append_batch(&batch).unwrap();
        assert_eq!(wal.appends_since_sync, 0, "crossing N must sync");

        fs::remove_file(&path).ok();
    }

    #[test]
    fn append_batch_encrypted_round_trip() {
        let path = temp_wal_path("batch_encrypted");
        let _ = fs::remove_file(&path);

        let batch: Vec<WalEntry> = (0..8)
            .map(|i| make_entry(200 + i, 0, format!("secret-{i}").as_bytes()))
            .collect();
        {
            let mut wal =
                Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x99))).unwrap();
            wal.append_batch(&batch).unwrap();
        }

        // Ciphertext on disk: plaintext payloads must not be visible.
        let raw = fs::read(&path).unwrap();
        assert!(
            !raw.windows(b"secret-0".len()).any(|w| w == b"secret-0"),
            "payload leaked to disk in plaintext"
        );

        let wal = Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x99))).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(out, batch);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn appended_entries_replayable_in_all_modes() {
        for (name, mode) in [
            ("mode_always", SyncMode::Always),
            ("mode_every_n", SyncMode::EveryN(4)),
            ("mode_off", SyncMode::Off),
        ] {
            let path = temp_wal_path(name);
            let _ = fs::remove_file(&path);

            let entries_in: Vec<WalEntry> = (0..7).map(|i| make_entry(i, 0, b"payload")).collect();
            {
                let mut wal = Wal::open_with_mode(&path, mode).unwrap();
                for e in &entries_in {
                    wal.append(e).unwrap();
                }
            }

            let wal = Wal::open(&path).unwrap();
            let out: Vec<WalEntry> = wal
                .replay()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(out, entries_in, "mode {mode:?}");

            fs::remove_file(&path).ok();
        }
    }

    #[test]
    fn recover_intact_log_returns_all_entries() {
        let path = temp_wal_path("recover_intact");
        let _ = fs::remove_file(&path);

        let entries_in: Vec<WalEntry> = (0..5).map(|i| make_entry(i, 0, b"data")).collect();
        {
            let mut wal = Wal::open(&path).unwrap();
            for e in &entries_in {
                wal.append(e).unwrap();
            }
        }

        let mut wal = Wal::open(&path).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, entries_in);
        assert_eq!(stats.entries_recovered, 5);
        assert_eq!(stats.truncated_at, None);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn recover_truncates_torn_tail() {
        let path = temp_wal_path("recover_torn_tail");
        let _ = fs::remove_file(&path);

        let e1 = make_entry(1, 0, b"complete-1");
        let e2 = make_entry(2, 0, b"complete-2");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&e1).unwrap();
            wal.append(&e2).unwrap();
        }

        // Simulate a torn write: append half of a third entry.
        let e3_bytes = make_entry(3, 0, b"torn-entry").encode_to_vec();
        let valid_len = fs::metadata(&path).unwrap().len();
        let mut data = fs::read(&path).unwrap();
        data.extend_from_slice(&e3_bytes[..e3_bytes.len() / 2]);
        fs::write(&path, &data).unwrap();

        let mut wal = Wal::open(&path).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, vec![e1.clone(), e2.clone()]);
        assert_eq!(stats.truncated_at, Some(valid_len));

        // File must be truncated back to the valid prefix.
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);

        // Appends after recovery produce a clean, fully-replayable log.
        let e4 = make_entry(4, 0, b"after-recovery");
        wal.append(&e4).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(out, vec![e1, e2, e4]);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn recover_truncates_corrupt_tail_checksum() {
        let path = temp_wal_path("recover_corrupt_tail");
        let _ = fs::remove_file(&path);

        let e1 = make_entry(1, 0, b"good");
        let e2 = make_entry(2, 0, b"will-be-corrupted");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&e1).unwrap();
            wal.append(&e2).unwrap();
        }

        // Corrupt a byte inside the second entry's payload.
        let e1_len = e1.encoded_size() as u64;
        let mut data = fs::read(&path).unwrap();
        let idx = e1.encoded_size() + 32; // inside e2
        data[idx] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut wal = Wal::open(&path).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, vec![e1]);
        assert_eq!(stats.truncated_at, Some(e1_len));
        assert_eq!(fs::metadata(&path).unwrap().len(), e1_len);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn recover_empty_log() {
        let path = temp_wal_path("recover_empty");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert!(entries.is_empty());
        assert_eq!(stats.entries_recovered, 0);
        assert_eq!(stats.truncated_at, None);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn preallocated_wal_replays_and_recovers() {
        let path = temp_wal_path("prealloc_basic");
        let _ = fs::remove_file(&path);

        let entries_in: Vec<WalEntry> = (0..5).map(|i| make_entry(i, 0, b"data")).collect();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.set_preallocate(64 * 1024);
            for e in &entries_in {
                wal.append(e).unwrap();
            }
            // File is preallocated well beyond the data written.
            assert_eq!(fs::metadata(&path).unwrap().len(), 64 * 1024);

            // Strict replay treats the zero tail as EOF.
            let out: Vec<WalEntry> = wal
                .replay()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(out, entries_in);
        }

        // Reopen: recover must not report the zero tail as damage.
        let mut wal = Wal::open(&path).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, entries_in);
        assert_eq!(stats.truncated_at, None);

        // Appends continue after the last valid entry, not at physical EOF.
        let extra = make_entry(99, 0, b"after-reopen");
        wal.append(&extra).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut expected = entries_in.clone();
        expected.push(extra);
        assert_eq!(out, expected);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn preallocation_grows_in_chunks() {
        let path = temp_wal_path("prealloc_chunks");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        wal.set_preallocate(256);

        // One append (< 256 bytes) → one chunk.
        wal.append(&make_entry(1, 0, b"x")).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 256);

        // Fill past the first chunk → grows by another chunk.
        for i in 0..8 {
            wal.append(&make_entry(2 + i, 0, &[0xAB; 32])).unwrap();
        }
        let len = fs::metadata(&path).unwrap().len();
        assert!(len > 256 && len % 256 == 0, "len = {len}");

        fs::remove_file(&path).ok();
    }

    #[test]
    fn reset_clears_preallocated_file() {
        let path = temp_wal_path("prealloc_reset");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        wal.set_preallocate(4096);
        wal.append(&make_entry(1, 0, b"data")).unwrap();
        wal.reset().unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);

        let e = make_entry(2, 0, b"fresh");
        wal.append(&e).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(out, vec![e]);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn recover_truncates_torn_write_inside_preallocated_region() {
        let path = temp_wal_path("prealloc_torn");
        let _ = fs::remove_file(&path);

        let e1 = make_entry(1, 0, b"good");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.set_preallocate(4096);
            wal.append(&e1).unwrap();
        }

        // Simulate a torn write: partial entry bytes right after e1,
        // followed by the remaining zero tail.
        let torn = make_entry(2, 0, b"torn").encode_to_vec();
        let valid_len = e1.encoded_size() as u64;
        let mut data = fs::read(&path).unwrap();
        let half = torn.len() / 2;
        data[valid_len as usize..valid_len as usize + half].copy_from_slice(&torn[..half]);
        fs::write(&path, &data).unwrap();

        let mut wal = Wal::open(&path).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, vec![e1]);
        assert_eq!(stats.truncated_at, Some(valid_len));
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);

        fs::remove_file(&path).ok();
    }

    // ── Encryption at rest (F4) ──

    fn test_cipher(byte: u8) -> Arc<Cipher> {
        Arc::new(Cipher::new(&crate::crypto::EncryptionKey::from_bytes(
            [byte; 32],
        )))
    }

    #[test]
    fn encrypted_append_and_replay_round_trip() {
        let path = temp_wal_path("enc_round_trip");
        let _ = fs::remove_file(&path);

        let entries_in: Vec<WalEntry> = (0..20)
            .map(|i| make_entry(1000 + i, 0, format!("secret-{i}").as_bytes()))
            .collect();
        {
            let mut wal =
                Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x11))).unwrap();
            for e in &entries_in {
                wal.append(e).unwrap();
            }
        }

        let wal = Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x11))).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(out, entries_in);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn encrypted_wal_file_contains_no_plaintext() {
        let path = temp_wal_path("enc_no_plaintext");
        let _ = fs::remove_file(&path);

        let secret = b"raft-top-secret-payload";
        let mut wal =
            Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x22))).unwrap();
        wal.append(&make_entry(1, 0, secret)).unwrap();

        let raw = fs::read(&path).unwrap();
        assert!(
            !raw.windows(secret.len()).any(|w| w == secret.as_slice()),
            "payload bytes must not appear in the encrypted file"
        );

        fs::remove_file(&path).ok();
    }

    #[test]
    fn encrypted_wal_unreadable_with_wrong_key() {
        let path = temp_wal_path("enc_wrong_key");
        let _ = fs::remove_file(&path);

        {
            let mut wal =
                Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x33))).unwrap();
            wal.append(&make_entry(1, 0, b"data")).unwrap();
        }

        let wal = Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x44))).unwrap();
        let results: Vec<_> = wal.replay().unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0],
            Err(WalError::BadCiphertext { .. }) | Err(WalError::PayloadTooLarge { .. })
        ));

        fs::remove_file(&path).ok();
    }

    #[test]
    fn corrupt_ciphertext_returns_integrity_error() {
        let path = temp_wal_path("enc_corrupt");
        let _ = fs::remove_file(&path);

        {
            let mut wal =
                Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x55))).unwrap();
            wal.append(&make_entry(1, 0, b"important encrypted data"))
                .unwrap();
        }

        // Flip one ciphertext byte (past the 4-byte frame header + nonce).
        let mut data = fs::read(&path).unwrap();
        let idx = 4 + crate::crypto::NONCE_LEN + 3;
        data[idx] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let wal = Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x55))).unwrap();
        let results: Vec<_> = wal.replay().unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(WalError::BadCiphertext { .. })));

        fs::remove_file(&path).ok();
    }

    #[test]
    fn encrypted_recover_truncates_torn_tail() {
        let path = temp_wal_path("enc_torn_tail");
        let _ = fs::remove_file(&path);

        let e1 = make_entry(1, 0, b"complete");
        {
            let mut wal =
                Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x66))).unwrap();
            wal.append(&e1).unwrap();
            wal.append(&make_entry(2, 0, b"will-be-torn")).unwrap();
        }

        // Tear the second frame in half.
        let data = fs::read(&path).unwrap();
        let e1_frame_end = {
            let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
            4 + len
        };
        let torn_len = e1_frame_end + (data.len() - e1_frame_end) / 2;
        fs::write(&path, &data[..torn_len]).unwrap();

        let mut wal =
            Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x66))).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, vec![e1.clone()]);
        assert_eq!(stats.truncated_at, Some(e1_frame_end as u64));
        assert_eq!(fs::metadata(&path).unwrap().len(), e1_frame_end as u64);

        // Appends after recovery produce a clean, fully-replayable log.
        let e3 = make_entry(3, 0, b"after-recovery");
        wal.append(&e3).unwrap();
        let out: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(out, vec![e1, e3]);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn encrypted_preallocated_wal_replays_and_recovers() {
        let path = temp_wal_path("enc_prealloc");
        let _ = fs::remove_file(&path);

        let entries_in: Vec<WalEntry> = (0..5).map(|i| make_entry(i, 0, b"data")).collect();
        {
            let mut wal =
                Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x77))).unwrap();
            wal.set_preallocate(64 * 1024);
            for e in &entries_in {
                wal.append(e).unwrap();
            }
        }

        let mut wal =
            Wal::open_with_cipher(&path, SyncMode::Always, Some(test_cipher(0x77))).unwrap();
        let (entries, stats) = wal.recover().unwrap();
        assert_eq!(entries, entries_in);
        assert_eq!(stats.truncated_at, None, "zero tail is not damage");

        fs::remove_file(&path).ok();
    }

    #[test]
    fn sync_does_not_error() {
        let path = temp_wal_path("sync_test");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        wal.append(&make_entry(1, 0, b"data")).unwrap();
        wal.sync().expect("sync should succeed");

        fs::remove_file(&path).ok();
    }
}
