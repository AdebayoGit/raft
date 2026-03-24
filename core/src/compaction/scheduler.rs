use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::error::CompactionError;
use super::merge::k_way_merge;
use crate::sstable::{SSTableReader, SSTableWriter};

/// Tunable knobs for the levelled compaction strategy.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of SSTables at a level before compaction triggers.
    /// Default: 4.
    pub level_threshold: usize,
    /// Maximum number of levels. SSTables at the deepest level accumulate
    /// without further compaction. Default: 7.
    pub max_levels: usize,
    /// Target data-block size in the output SSTable. Default: 4096.
    pub block_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            level_threshold: 4,
            max_levels: 7,
            block_size: 4096,
        }
    }
}

/// Statistics returned after a compaction pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionStats {
    /// Number of levels that were compacted.
    pub levels_compacted: usize,
    /// Total SSTables consumed (merged).
    pub tables_merged: usize,
    /// Total SSTables produced.
    pub tables_written: usize,
    /// Total SSTables deleted from disk.
    pub tables_deleted: usize,
}

/// Levelled compaction scheduler.
///
/// Manages a hierarchy of SSTable levels under a root directory:
///
/// ```text
/// <db_dir>/
///   L0/
///     000001.sst
///     000002.sst
///   L1/
///     000003.sst
///   ...
/// ```
///
/// Call `add_sstable()` after flushing a memtable, then `run_if_idle()`
/// when the device is idle.
pub struct CompactionScheduler {
    db_dir: PathBuf,
    config: CompactionConfig,
    /// `levels[i]` holds the file paths of SSTables at level i, ordered
    /// from oldest to newest.
    levels: Vec<Vec<PathBuf>>,
    /// Monotonically increasing counter for generating unique file names.
    next_id: u64,
}

impl CompactionScheduler {
    /// Create a scheduler rooted at `db_dir` with the given configuration.
    ///
    /// Creates the directory structure if it doesn't exist.
    pub fn open(db_dir: impl AsRef<Path>, config: CompactionConfig) -> Result<Self, CompactionError> {
        let db_dir = db_dir.as_ref().to_path_buf();
        let mut levels = Vec::with_capacity(config.max_levels);
        let mut max_id: u64 = 0;

        for l in 0..config.max_levels {
            let level_dir = db_dir.join(format!("L{l}"));
            fs::create_dir_all(&level_dir)?;

            let mut files: Vec<PathBuf> = Vec::new();
            for entry in fs::read_dir(&level_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "sst") {
                    // Extract numeric id from filename for counter tracking.
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(id) = stem.parse::<u64>() {
                            max_id = max_id.max(id);
                        }
                    }
                    files.push(path);
                }
            }
            // Sort by filename to maintain oldest → newest order.
            files.sort();
            levels.push(files);
        }

        Ok(Self {
            db_dir,
            config,
            levels,
            next_id: max_id + 1,
        })
    }

    /// Register a newly flushed SSTable at level 0.
    ///
    /// The file must already exist on disk. The scheduler just tracks it.
    pub fn add_sstable(&mut self, path: PathBuf) {
        self.levels[0].push(path);
    }

    /// Flush a memtable iterator directly to L0 and register it.
    ///
    /// Convenience method that writes via `SSTableWriter` and then tracks
    /// the resulting file.
    pub fn flush_to_l0(
        &mut self,
        iter: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<PathBuf, CompactionError> {
        let path = self.next_path(0);
        let writer = SSTableWriter::new(&path).with_block_size(self.config.block_size);
        writer.write(iter)?;
        self.levels[0].push(path.clone());
        Ok(path)
    }

    /// Run one compaction pass if any level exceeds its threshold.
    ///
    /// Designed to be called when the system detects low activity. Does
    /// nothing if no level needs compaction, making it cheap to call often.
    ///
    /// Returns statistics about what work was performed.
    pub fn run_if_idle(&mut self) -> Result<CompactionStats, CompactionError> {
        let mut stats = CompactionStats::default();
        let _start = Instant::now();

        // Walk levels 0..max-2, compacting any that exceed threshold.
        // Stop after one level per call to bound the amount of work.
        for level in 0..self.config.max_levels.saturating_sub(1) {
            if self.levels[level].len() >= self.config.level_threshold {
                self.compact_level(level, &mut stats)?;
                break;
            }
        }

        Ok(stats)
    }

    /// Force compaction at a specific level regardless of threshold.
    ///
    /// Merges all SSTables at `level` into a single SSTable and promotes it
    /// to `level + 1`. Tables already at the target level are left untouched
    /// — they accumulate until the target level itself exceeds its threshold.
    ///
    /// Useful for testing and manual maintenance.
    pub fn compact_level(
        &mut self,
        level: usize,
        stats: &mut CompactionStats,
    ) -> Result<(), CompactionError> {
        let next_level = level + 1;
        if next_level >= self.config.max_levels {
            return Ok(()); // deepest level — nothing to do
        }
        if self.levels[level].is_empty() {
            return Ok(());
        }

        // Read all entries from tables at this level.
        // Ordering: index 0 = oldest, index N = newest.
        let mut all_inputs: Vec<Vec<super::merge::KvPair>> = Vec::new();
        for path in &self.levels[level] {
            let reader = SSTableReader::open(path)?;
            all_inputs.push(reader.scan_all()?);
        }

        let merged = k_way_merge(all_inputs);

        // Write merged output to the next level.
        let out_path = self.next_path(next_level);
        if !merged.is_empty() {
            let writer = SSTableWriter::new(&out_path).with_block_size(self.config.block_size);
            writer.write(merged.into_iter())?;
            stats.tables_written += 1;
        }

        // Delete consumed inputs from this level.
        let consumed: Vec<PathBuf> = self.levels[level].drain(..).collect();
        for path in &consumed {
            fs::remove_file(path).ok();
            stats.tables_deleted += 1;
        }
        stats.tables_merged += consumed.len();

        // Register the new output at the target level.
        if out_path.exists() {
            self.levels[next_level].push(out_path);
        }

        stats.levels_compacted += 1;
        Ok(())
    }

    /// Number of SSTables at a given level.
    pub fn level_size(&self, level: usize) -> usize {
        self.levels.get(level).map_or(0, Vec::len)
    }

    /// Total SSTables across all levels.
    pub fn total_tables(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    /// Whether any level (except the deepest) exceeds its threshold.
    pub fn needs_compaction(&self) -> bool {
        for level in 0..self.config.max_levels.saturating_sub(1) {
            if self.levels[level].len() >= self.config.level_threshold {
                return true;
            }
        }
        false
    }

    /// Generate the next unique SSTable path for a given level.
    fn next_path(&mut self, level: usize) -> PathBuf {
        let id = self.next_id;
        self.next_id += 1;
        let level_dir = self.db_dir.join(format!("L{level}"));
        level_dir.join(format!("{id:06}.sst"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("raft_db_compaction_tests")
            .join(name);
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        dir
    }

    fn sample_entries(prefix: &str, n: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        (0..n)
            .map(|i| {
                (
                    format!("{prefix}-{i:05}").into_bytes(),
                    Some(format!("val-{i:05}").into_bytes()),
                )
            })
            .collect()
    }

    fn small_config() -> CompactionConfig {
        CompactionConfig {
            level_threshold: 2,
            max_levels: 4,
            block_size: 128,
        }
    }

    #[test]
    fn open_creates_level_dirs() {
        let dir = temp_dir("open_creates");
        let sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        for l in 0..4 {
            assert!(dir.join(format!("L{l}")).is_dir());
        }
        assert_eq!(sched.total_tables(), 0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn flush_to_l0_registers_table() {
        let dir = temp_dir("flush_l0");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        let entries = sample_entries("k", 10);
        let path = sched.flush_to_l0(entries.into_iter()).unwrap();

        assert!(path.exists());
        assert_eq!(sched.level_size(0), 1);
        assert_eq!(sched.total_tables(), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn run_if_idle_no_op_when_under_threshold() {
        let dir = temp_dir("no_op");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        sched
            .flush_to_l0(sample_entries("a", 5).into_iter())
            .unwrap();

        assert!(!sched.needs_compaction());
        let stats = sched.run_if_idle().unwrap();
        assert_eq!(stats.levels_compacted, 0);
        assert_eq!(sched.level_size(0), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_triggers_at_threshold() {
        let dir = temp_dir("triggers");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        // Flush two L0 tables (threshold = 2).
        sched
            .flush_to_l0(sample_entries("a", 5).into_iter())
            .unwrap();
        sched
            .flush_to_l0(sample_entries("b", 5).into_iter())
            .unwrap();

        assert!(sched.needs_compaction());
        assert_eq!(sched.level_size(0), 2);

        let stats = sched.run_if_idle().unwrap();

        assert_eq!(stats.levels_compacted, 1);
        assert_eq!(stats.tables_merged, 2);
        assert_eq!(stats.tables_written, 1);
        assert_eq!(stats.tables_deleted, 2);

        // L0 is now empty, L1 has the merged table.
        assert_eq!(sched.level_size(0), 0);
        assert_eq!(sched.level_size(1), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn merged_table_contains_all_keys() {
        let dir = temp_dir("merged_keys");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        sched
            .flush_to_l0(
                vec![
                    (b"a".to_vec(), Some(b"1".to_vec())),
                    (b"c".to_vec(), Some(b"3".to_vec())),
                ]
                .into_iter(),
            )
            .unwrap();
        sched
            .flush_to_l0(
                vec![
                    (b"b".to_vec(), Some(b"2".to_vec())),
                    (b"d".to_vec(), Some(b"4".to_vec())),
                ]
                .into_iter(),
            )
            .unwrap();

        sched.run_if_idle().unwrap();

        // Read the merged L1 table and verify all 4 keys.
        let l1_path = &sched.levels[1][0];
        let reader = SSTableReader::open(l1_path).unwrap();
        assert_eq!(reader.entry_count(), 4);
        assert_eq!(reader.get(b"a").unwrap(), Some(Some(b"1".to_vec())));
        assert_eq!(reader.get(b"b").unwrap(), Some(Some(b"2".to_vec())));
        assert_eq!(reader.get(b"c").unwrap(), Some(Some(b"3".to_vec())));
        assert_eq!(reader.get(b"d").unwrap(), Some(Some(b"4".to_vec())));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn newer_value_wins_on_duplicate_key() {
        let dir = temp_dir("newer_wins");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        // Older flush.
        sched
            .flush_to_l0(vec![(b"key".to_vec(), Some(b"old".to_vec()))].into_iter())
            .unwrap();
        // Newer flush.
        sched
            .flush_to_l0(vec![(b"key".to_vec(), Some(b"new".to_vec()))].into_iter())
            .unwrap();

        sched.run_if_idle().unwrap();

        let reader = SSTableReader::open(&sched.levels[1][0]).unwrap();
        assert_eq!(reader.get(b"key").unwrap(), Some(Some(b"new".to_vec())));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tombstone_preserved_through_compaction() {
        let dir = temp_dir("tombstone");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        sched
            .flush_to_l0(vec![(b"key".to_vec(), Some(b"alive".to_vec()))].into_iter())
            .unwrap();
        sched
            .flush_to_l0(vec![(b"key".to_vec(), None)].into_iter())
            .unwrap();

        sched.run_if_idle().unwrap();

        let reader = SSTableReader::open(&sched.levels[1][0]).unwrap();
        assert_eq!(reader.get(b"key").unwrap(), Some(None)); // tombstone wins

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cascading_compaction() {
        let dir = temp_dir("cascade");
        let config = CompactionConfig {
            level_threshold: 2,
            max_levels: 4,
            block_size: 128,
        };
        let mut sched = CompactionScheduler::open(&dir, config).unwrap();

        // Fill L0 → compact to L1 (round 1).
        sched
            .flush_to_l0(sample_entries("r1a", 3).into_iter())
            .unwrap();
        sched
            .flush_to_l0(sample_entries("r1b", 3).into_iter())
            .unwrap();
        sched.run_if_idle().unwrap();
        assert_eq!(sched.level_size(0), 0);
        assert_eq!(sched.level_size(1), 1);

        // Fill L0 → compact to L1 (round 2). L1 now has 2 → triggers L1→L2.
        sched
            .flush_to_l0(sample_entries("r2a", 3).into_iter())
            .unwrap();
        sched
            .flush_to_l0(sample_entries("r2b", 3).into_iter())
            .unwrap();
        sched.run_if_idle().unwrap(); // L0 → L1
        assert_eq!(sched.level_size(0), 0);
        assert_eq!(sched.level_size(1), 2);

        sched.run_if_idle().unwrap(); // L1 → L2
        assert_eq!(sched.level_size(1), 0);
        assert_eq!(sched.level_size(2), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_discovers_existing_tables() {
        let dir = temp_dir("reopen");
        let config = small_config();

        // Session 1: flush a table.
        {
            let mut sched = CompactionScheduler::open(&dir, config.clone()).unwrap();
            sched
                .flush_to_l0(sample_entries("x", 5).into_iter())
                .unwrap();
            assert_eq!(sched.level_size(0), 1);
        }

        // Session 2: reopen — should discover the existing table.
        {
            let sched = CompactionScheduler::open(&dir, config).unwrap();
            assert_eq!(sched.level_size(0), 1);
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn add_sstable_tracks_external_file() {
        let dir = temp_dir("add_ext");
        let mut sched = CompactionScheduler::open(&dir, small_config()).unwrap();

        // Write an SSTable manually outside the scheduler.
        let path = dir.join("L0").join("external.sst");
        let w = SSTableWriter::new(&path).with_block_size(128);
        w.write(sample_entries("e", 3).into_iter()).unwrap();

        sched.add_sstable(path);
        assert_eq!(sched.level_size(0), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deepest_level_accumulates() {
        let dir = temp_dir("deepest");
        let config = CompactionConfig {
            level_threshold: 1,
            max_levels: 2,
            block_size: 128,
        };
        let mut sched = CompactionScheduler::open(&dir, config).unwrap();

        // Flush to L0, compact to L1 (deepest).
        sched
            .flush_to_l0(sample_entries("a", 3).into_iter())
            .unwrap();
        sched.run_if_idle().unwrap();
        assert_eq!(sched.level_size(0), 0);
        assert_eq!(sched.level_size(1), 1);

        // Flush more, compact to L1 again — accumulates.
        sched
            .flush_to_l0(sample_entries("b", 3).into_iter())
            .unwrap();
        sched.run_if_idle().unwrap();
        assert_eq!(sched.level_size(1), 2); // two tables, no further cascade

        // L1 is the deepest level so it doesn't trigger compaction.
        assert!(!sched.needs_compaction());

        fs::remove_dir_all(&dir).ok();
    }
}
