//! Bounded, byte-capped LRU cache for SSTable data blocks.
//!
//! Shared across all [`SSTableReader`](super::SSTableReader)s of an engine
//! so total cached block bytes stay under a configurable cap regardless of
//! how many tables are live. Keys are `(table_id, block_offset)` — block
//! contents are immutable, so cached bytes never need invalidation except
//! when a table is deleted by compaction ([`BlockCache::evict_table`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Cache key: which table, which block within it.
type BlockKey = (u64, u64);

struct CacheEntry {
    block: Arc<Vec<u8>>,
    /// Recency stamp — larger is more recently used.
    last_used: u64,
}

struct CacheInner {
    map: HashMap<BlockKey, CacheEntry>,
    /// Total bytes of all cached blocks.
    bytes: usize,
    /// Monotonic recency counter.
    tick: u64,
}

/// A byte-capped LRU cache mapping `(table_id, offset)` to block bytes.
///
/// Lookups are O(1); eviction scans for the least-recently-used entry and
/// only runs when an insert exceeds the byte capacity. Blocks larger than
/// the whole capacity are returned to the caller but never cached.
pub struct BlockCache {
    inner: Mutex<CacheInner>,
    capacity_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("capacity_bytes", &self.capacity_bytes)
            .field("current_bytes", &self.current_bytes())
            .finish()
    }
}

impl BlockCache {
    /// Create a cache holding at most `capacity_bytes` of block data.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                bytes: 0,
                tick: 0,
            }),
            capacity_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Fetch a cached block, updating its recency.
    pub fn get(&self, table_id: u64, offset: u64) -> Option<Arc<Vec<u8>>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.tick += 1;
        let tick = inner.tick;
        match inner.map.get_mut(&(table_id, offset)) {
            Some(entry) => {
                entry.last_used = tick;
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(Arc::clone(&entry.block))
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a block, evicting least-recently-used entries until the
    /// total stays within the byte capacity. Oversized blocks (larger
    /// than the whole capacity) are not cached.
    pub fn insert(&self, table_id: u64, offset: u64, block: Arc<Vec<u8>>) {
        if block.len() > self.capacity_bytes {
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.tick += 1;
        let tick = inner.tick;

        // Replace any existing entry for this key first.
        if let Some(old) = inner.map.remove(&(table_id, offset)) {
            inner.bytes -= old.block.len();
        }

        // Evict LRU entries until the new block fits.
        while inner.bytes + block.len() > self.capacity_bytes {
            let Some((&victim, _)) = inner.map.iter().min_by_key(|(_, entry)| entry.last_used)
            else {
                break;
            };
            if let Some(evicted) = inner.map.remove(&victim) {
                inner.bytes -= evicted.block.len();
            }
        }

        inner.bytes += block.len();
        inner.map.insert(
            (table_id, offset),
            CacheEntry {
                block,
                last_used: tick,
            },
        );
    }

    /// Drop every cached block belonging to `table_id` — called when a
    /// table is deleted by compaction.
    pub fn evict_table(&self, table_id: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let victims: Vec<BlockKey> = inner
            .map
            .keys()
            .filter(|(id, _)| *id == table_id)
            .copied()
            .collect();
        for key in victims {
            if let Some(evicted) = inner.map.remove(&key) {
                inner.bytes -= evicted.block.len();
            }
        }
    }

    /// Total bytes currently cached. Always ≤ the configured capacity.
    pub fn current_bytes(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }

    /// Configured byte capacity.
    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    /// Number of cache hits since creation.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Number of cache misses since creation.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: usize) -> Arc<Vec<u8>> {
        Arc::new(vec![0xAB; n])
    }

    #[test]
    fn get_returns_inserted_block() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, block(100));
        assert_eq!(cache.get(1, 0).unwrap().len(), 100);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 0);
    }

    #[test]
    fn miss_on_absent_block() {
        let cache = BlockCache::new(1024);
        assert!(cache.get(1, 0).is_none());
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn byte_capacity_is_enforced() {
        let cache = BlockCache::new(250);
        cache.insert(1, 0, block(100));
        cache.insert(1, 100, block(100));
        cache.insert(1, 200, block(100)); // exceeds cap → evicts LRU
        assert!(cache.current_bytes() <= 250);
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn least_recently_used_is_evicted_first() {
        let cache = BlockCache::new(250);
        cache.insert(1, 0, block(100));
        cache.insert(1, 100, block(100));
        // Touch (1, 0) so (1, 100) becomes the LRU victim.
        assert!(cache.get(1, 0).is_some());
        cache.insert(1, 200, block(100));
        assert!(cache.get(1, 0).is_some(), "recently used must survive");
        assert!(cache.get(1, 100).is_none(), "LRU must be evicted");
    }

    #[test]
    fn oversized_block_is_not_cached() {
        let cache = BlockCache::new(50);
        cache.insert(1, 0, block(100));
        assert_eq!(cache.current_bytes(), 0);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn replacing_a_key_accounts_bytes_once() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, block(100));
        cache.insert(1, 0, block(200));
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn evict_table_drops_all_its_blocks() {
        let cache = BlockCache::new(1024);
        cache.insert(1, 0, block(100));
        cache.insert(1, 100, block(100));
        cache.insert(2, 0, block(100));
        cache.evict_table(1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 100).is_none());
        assert!(cache.get(2, 0).is_some());
        assert_eq!(cache.current_bytes(), 100);
    }
}
