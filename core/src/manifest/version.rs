use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use super::error::ManifestError;
use super::record::{ManifestRecord, SSTableMeta, TableId};

/// A point-in-time view of the database's SSTable layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbVersion {
    /// Monotonically increasing DB sequence number.
    pub sequence: u64,
    /// Live SSTables keyed by their unique id.
    pub tables: BTreeMap<TableId, SSTableMeta>,
}

impl DbVersion {
    fn new() -> Self {
        Self {
            sequence: 0,
            tables: BTreeMap::new(),
        }
    }
}

/// Persistent manifest that tracks live SSTables and DB state.
///
/// Writes are appended as checksummed binary records. On `open()` the log
/// is replayed to reconstruct the current `DbVersion`.
pub struct Manifest {
    path: PathBuf,
    file: File,
    version: DbVersion,
    /// Counter of records written since the last snapshot. Used to decide
    /// when to write a compaction snapshot.
    records_since_snapshot: usize,
}

/// Number of incremental records before we auto-write a snapshot to bound
/// recovery time.
const SNAPSHOT_INTERVAL: usize = 64;

impl Manifest {
    /// Open (or create) the manifest at `path`, replaying existing records
    /// to recover the current database version.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref().to_path_buf();

        let mut version = DbVersion::new();
        let mut records_since_snapshot: usize = 0;

        // Replay existing log if the file exists and is non-empty.
        if path.exists() {
            let data = std::fs::read(&path)?;
            if !data.is_empty() {
                let mut cursor: &[u8] = &data;
                let mut offset: u64 = 0;
                while let Some(record) = ManifestRecord::decode(&mut cursor, offset)? {
                    Self::apply(&mut version, &record)?;
                    offset = (data.len() - cursor.len()) as u64;
                    records_since_snapshot += 1;
                }
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            file,
            version,
            records_since_snapshot,
        })
    }

    /// Register a new SSTable in the manifest.
    ///
    /// Returns an error if the table id is already live (duplicate).
    pub fn add_sstable(&mut self, meta: SSTableMeta) -> Result<(), ManifestError> {
        if self.version.tables.contains_key(&meta.id) {
            return Err(ManifestError::DuplicateTable(meta.id));
        }

        let record = ManifestRecord::AddTable(meta);
        self.write_record(&record)?;
        Self::apply(&mut self.version, &record)?;
        self.maybe_snapshot()?;
        Ok(())
    }

    /// Remove an SSTable from the manifest (e.g. after compaction).
    ///
    /// Returns an error if the table id is not found.
    pub fn remove_sstable(&mut self, id: TableId) -> Result<(), ManifestError> {
        if !self.version.tables.contains_key(&id) {
            return Err(ManifestError::TableNotFound(id));
        }

        let record = ManifestRecord::RemoveTable(id);
        self.write_record(&record)?;
        Self::apply(&mut self.version, &record)?;
        self.maybe_snapshot()?;
        Ok(())
    }

    /// Advance the DB-wide sequence number.
    pub fn set_sequence(&mut self, seq: u64) -> Result<(), ManifestError> {
        let record = ManifestRecord::SetSequence(seq);
        self.write_record(&record)?;
        Self::apply(&mut self.version, &record)?;
        Ok(())
    }

    /// Return a clone of the current database version.
    pub fn current_version(&self) -> DbVersion {
        self.version.clone()
    }

    /// Path to the manifest file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of live SSTables.
    pub fn table_count(&self) -> usize {
        self.version.tables.len()
    }

    /// All live SSTable metadata, ordered by id.
    pub fn tables(&self) -> Vec<&SSTableMeta> {
        self.version.tables.values().collect()
    }

    /// Tables at a specific compaction level, ordered by id.
    pub fn tables_at_level(&self, level: u32) -> Vec<&SSTableMeta> {
        self.version
            .tables
            .values()
            .filter(|t| t.level == level)
            .collect()
    }

    /// Write a full snapshot record to the log and reset the counter.
    /// This bounds recovery time by allowing the reader to skip earlier
    /// incremental records.
    pub fn write_snapshot(&mut self) -> Result<(), ManifestError> {
        let record = ManifestRecord::Snapshot {
            sequence: self.version.sequence,
            tables: self.version.tables.values().cloned().collect(),
        };
        self.write_record(&record)?;
        self.records_since_snapshot = 0;
        Ok(())
    }

    // ── Internal helpers ──

    fn write_record(&mut self, record: &ManifestRecord) -> Result<(), ManifestError> {
        let encoded = record.encode();
        self.file.write_all(&encoded)?;
        self.file.flush()?;
        Ok(())
    }

    fn maybe_snapshot(&mut self) -> Result<(), ManifestError> {
        self.records_since_snapshot += 1;
        if self.records_since_snapshot >= SNAPSHOT_INTERVAL {
            self.write_snapshot()?;
        }
        Ok(())
    }

    /// Apply a record to a version in memory.
    fn apply(version: &mut DbVersion, record: &ManifestRecord) -> Result<(), ManifestError> {
        match record {
            ManifestRecord::AddTable(meta) => {
                version.tables.insert(meta.id, meta.clone());
            }
            ManifestRecord::RemoveTable(id) => {
                version.tables.remove(id);
            }
            ManifestRecord::SetSequence(seq) => {
                version.sequence = *seq;
            }
            ManifestRecord::Snapshot { sequence, tables } => {
                version.sequence = *sequence;
                version.tables.clear();
                for t in tables {
                    version.tables.insert(t.id, t.clone());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("raft_db_manifest_tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{name}.manifest"))
    }

    fn meta(id: TableId, level: u32) -> SSTableMeta {
        SSTableMeta {
            id,
            level,
            smallest_key: format!("k{id:03}-first").into_bytes(),
            largest_key: format!("k{id:03}-last").into_bytes(),
            entry_count: 100,
            file_size: 4096,
        }
    }

    #[test]
    fn open_creates_empty_manifest() {
        let path = temp_path("open_empty");
        let _ = fs::remove_file(&path);

        let m = Manifest::open(&path).unwrap();
        assert_eq!(m.table_count(), 0);
        assert_eq!(m.current_version().sequence, 0);
        assert!(path.exists());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn add_and_query_sstable() {
        let path = temp_path("add_query");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable(meta(1, 0)).unwrap();
        m.add_sstable(meta(2, 0)).unwrap();
        m.add_sstable(meta(3, 1)).unwrap();

        assert_eq!(m.table_count(), 3);

        let v = m.current_version();
        assert!(v.tables.contains_key(&1));
        assert!(v.tables.contains_key(&2));
        assert!(v.tables.contains_key(&3));

        assert_eq!(m.tables_at_level(0).len(), 2);
        assert_eq!(m.tables_at_level(1).len(), 1);
        assert_eq!(m.tables_at_level(2).len(), 0);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn add_duplicate_returns_error() {
        let path = temp_path("add_dup");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable(meta(1, 0)).unwrap();
        let result = m.add_sstable(meta(1, 0));
        assert!(matches!(result, Err(ManifestError::DuplicateTable(1))));

        fs::remove_file(&path).ok();
    }

    #[test]
    fn remove_sstable() {
        let path = temp_path("remove");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable(meta(1, 0)).unwrap();
        m.add_sstable(meta(2, 0)).unwrap();

        m.remove_sstable(1).unwrap();
        assert_eq!(m.table_count(), 1);
        assert!(!m.current_version().tables.contains_key(&1));
        assert!(m.current_version().tables.contains_key(&2));

        fs::remove_file(&path).ok();
    }

    #[test]
    fn remove_nonexistent_returns_error() {
        let path = temp_path("remove_missing");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();
        let result = m.remove_sstable(999);
        assert!(matches!(result, Err(ManifestError::TableNotFound(999))));

        fs::remove_file(&path).ok();
    }

    #[test]
    fn set_sequence() {
        let path = temp_path("set_seq");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();
        assert_eq!(m.current_version().sequence, 0);

        m.set_sequence(42).unwrap();
        assert_eq!(m.current_version().sequence, 42);

        m.set_sequence(100).unwrap();
        assert_eq!(m.current_version().sequence, 100);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn persists_across_reopen() {
        let path = temp_path("reopen");
        let _ = fs::remove_file(&path);

        // Session 1: write some state.
        {
            let mut m = Manifest::open(&path).unwrap();
            m.set_sequence(10).unwrap();
            m.add_sstable(meta(1, 0)).unwrap();
            m.add_sstable(meta(2, 1)).unwrap();
        }

        // Session 2: reopen and verify.
        {
            let m = Manifest::open(&path).unwrap();
            let v = m.current_version();
            assert_eq!(v.sequence, 10);
            assert_eq!(m.table_count(), 2);
            assert!(v.tables.contains_key(&1));
            assert!(v.tables.contains_key(&2));
        }

        fs::remove_file(&path).ok();
    }

    #[test]
    fn remove_persists_across_reopen() {
        let path = temp_path("reopen_remove");
        let _ = fs::remove_file(&path);

        {
            let mut m = Manifest::open(&path).unwrap();
            m.add_sstable(meta(1, 0)).unwrap();
            m.add_sstable(meta(2, 0)).unwrap();
            m.remove_sstable(1).unwrap();
        }

        {
            let m = Manifest::open(&path).unwrap();
            assert_eq!(m.table_count(), 1);
            assert!(!m.current_version().tables.contains_key(&1));
            assert!(m.current_version().tables.contains_key(&2));
        }

        fs::remove_file(&path).ok();
    }

    #[test]
    fn snapshot_write_and_recovery() {
        let path = temp_path("snapshot_recovery");
        let _ = fs::remove_file(&path);

        {
            let mut m = Manifest::open(&path).unwrap();
            m.set_sequence(50).unwrap();
            m.add_sstable(meta(1, 0)).unwrap();
            m.add_sstable(meta(2, 1)).unwrap();
            m.write_snapshot().unwrap();
            // Add more after the snapshot.
            m.add_sstable(meta(3, 0)).unwrap();
        }

        {
            let m = Manifest::open(&path).unwrap();
            let v = m.current_version();
            assert_eq!(v.sequence, 50);
            assert_eq!(m.table_count(), 3);
        }

        fs::remove_file(&path).ok();
    }

    #[test]
    fn tables_ordered_by_id() {
        let path = temp_path("ordered");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();
        m.add_sstable(meta(5, 0)).unwrap();
        m.add_sstable(meta(1, 0)).unwrap();
        m.add_sstable(meta(3, 0)).unwrap();

        let ids: Vec<TableId> = m.tables().iter().map(|t| t.id).collect();
        assert_eq!(ids, vec![1, 3, 5]);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn metadata_preserved_correctly() {
        let path = temp_path("meta_check");
        let _ = fs::remove_file(&path);

        let original = SSTableMeta {
            id: 7,
            level: 2,
            smallest_key: b"aaa".to_vec(),
            largest_key: b"zzz".to_vec(),
            entry_count: 12345,
            file_size: 67890,
        };

        {
            let mut m = Manifest::open(&path).unwrap();
            m.add_sstable(original.clone()).unwrap();
        }

        {
            let m = Manifest::open(&path).unwrap();
            let recovered = &m.current_version().tables[&7];
            assert_eq!(*recovered, original);
        }

        fs::remove_file(&path).ok();
    }

    #[test]
    fn detects_corruption() {
        let path = temp_path("corrupt");
        let _ = fs::remove_file(&path);

        {
            let mut m = Manifest::open(&path).unwrap();
            m.add_sstable(meta(1, 0)).unwrap();
        }

        // Corrupt the file.
        let mut data = fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = Manifest::open(&path);
        assert!(result.is_err());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn many_adds_and_removes() {
        let path = temp_path("many_ops");
        let _ = fs::remove_file(&path);

        let mut m = Manifest::open(&path).unwrap();

        for i in 1..=50 {
            m.add_sstable(meta(i, (i % 3) as u32)).unwrap();
        }
        assert_eq!(m.table_count(), 50);

        // Remove even ids.
        for i in (2..=50).step_by(2) {
            m.remove_sstable(i).unwrap();
        }
        assert_eq!(m.table_count(), 25);

        // Verify remaining are odd ids.
        let ids: Vec<TableId> = m.tables().iter().map(|t| t.id).collect();
        assert!(ids.iter().all(|id| id % 2 == 1));

        drop(m);

        // Reopen and verify.
        let m = Manifest::open(&path).unwrap();
        assert_eq!(m.table_count(), 25);

        fs::remove_file(&path).ok();
    }
}
