use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use super::entry::WalEntry;
use super::error::WalError;

/// Write-ahead log backed by a single append-only file.
///
/// All mutations flow through the WAL before reaching the memtable.
/// On recovery, `replay()` reads every entry back in order.
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    /// Open (or create) a WAL file at `path`.
    ///
    /// The file is opened in append mode — existing data is preserved.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self { path, writer })
    }

    /// Append a single entry to the log and flush to disk.
    pub fn append(&mut self, entry: &WalEntry) -> Result<(), WalError> {
        let encoded = entry.encode_to_vec();
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Replay the entire log, yielding entries in append order.
    ///
    /// Opens a fresh read handle so it can be called while the writer is live.
    pub fn replay(&self) -> Result<WalIterator, WalError> {
        let mut file = File::open(&self.path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(WalIterator { data, pos: 0 })
    }

    /// Sync the underlying file to durable storage.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

/// Iterator over WAL entries read from a snapshot of the log file.
pub struct WalIterator {
    data: Vec<u8>,
    pos: usize,
}

impl Iterator for WalIterator {
    type Item = Result<WalEntry, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }

        let mut cursor = &self.data[self.pos..];
        let offset = self.pos as u64;

        match WalEntry::decode(&mut cursor, offset) {
            Ok(Some(entry)) => {
                self.pos = self.data.len() - cursor.len();
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
    fn sync_does_not_error() {
        let path = temp_wal_path("sync_test");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        wal.append(&make_entry(1, 0, b"data")).unwrap();
        wal.sync().expect("sync should succeed");

        fs::remove_file(&path).ok();
    }
}
