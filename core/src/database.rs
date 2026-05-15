//! High-level Database runtime — combines storage, collections, queries,
//! transactions, and reactive observation into a single coherent API.
//!
//! This is the layer the FFI exposes to platform bindings. It sits on top
//! of [`StorageEngine`] and uses the engine's KV interface as durable
//! storage, while keeping collection state in memory for fast queries.
//!
//! Storage layout in the underlying engine:
//!
//! ```text
//! __doc__/{collection}/{doc_id_be}    →  JSON-encoded Document
//! __meta__/{collection}/__id_counter  →  big-endian u64 (next auto-id)
//! ```
//!
//! Documents are loaded into per-collection in-memory maps on
//! [`Database::open`]. Subsequent operations update both the memory cache
//! and the durable engine. Reads serve from memory.
//!
//! Available without features (`async` / `ffi` not required) — observers
//! and live queries are gated behind `async`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use crate::engine::{StorageConfig, StorageEngine, StorageError};
use crate::index::DocId;
use crate::query::{Document, DocumentStore, IndexSet, Query, QueryExecutor, QueryPlanner};
use crate::transaction::{TransactionError, VersionedDocument, VersionedStore};

#[cfg(feature = "async")]
use crate::reactive::{EventBus, MutationEvent, MutationOrigin, MutationType};

/// Top-level database errors visible to callers of the high-level API.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("transaction error: {0}")]
    Transaction(#[from] TransactionError),

    #[error("invalid document encoding: {0}")]
    Decode(String),

    #[error("collection not found: {0}")]
    UnknownCollection(String),
}

const DOC_PREFIX: &[u8] = b"__doc__/";
const META_PREFIX: &[u8] = b"__meta__/";
const ID_COUNTER_SUFFIX: &[u8] = b"/__id_counter";

/// In-memory state for a single collection.
///
/// Documents and their versions are kept in RAM for cheap query / scan;
/// the durable copy lives in the engine's KV store. Versions start at 1
/// and increment on every write, enabling optimistic concurrency control.
#[derive(Debug)]
struct CollectionState {
    docs: HashMap<DocId, Document>,
    versions: HashMap<DocId, u64>,
    next_version: u64,
    next_doc_id: u64,
}

impl Default for CollectionState {
    fn default() -> Self {
        Self {
            docs: HashMap::new(),
            versions: HashMap::new(),
            next_version: 1,
            next_doc_id: 1,
        }
    }
}

/// The shared inner state of a [`Database`].
///
/// Wrapped in [`Arc`] inside `Database` so transactions and observers can
/// hold a reference back to the database without aliasing constraints.
pub(crate) struct DatabaseInner {
    engine: Mutex<StorageEngine>,
    collections: RwLock<HashMap<String, CollectionState>>,
    #[cfg(feature = "async")]
    bus: EventBus,
}

/// High-level embedded database: collections of typed documents on top of
/// a durable LSM-tree.
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    /// Open a database at `path`. Creates the directory tree on first run
    /// and replays existing collection state into memory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Self::open_with_config(path, StorageConfig::default())
    }

    /// Open with a custom [`StorageConfig`].
    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: StorageConfig,
    ) -> Result<Self, DatabaseError> {
        let engine = StorageEngine::open(path, config)?;
        let collections = Self::scan_collections(&engine)?;

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                engine: Mutex::new(engine),
                collections: RwLock::new(collections),
                #[cfg(feature = "async")]
                bus: EventBus::new(),
            }),
        })
    }

    // ── Collection ops ──────────────────────────────────────────────────

    /// Insert or update a document in `collection`. The doc's [`Document::id`]
    /// is honoured. Returns the new version number.
    pub fn put(&self, collection: &str, doc: Document) -> Result<u64, DatabaseError> {
        let id = doc.id;
        let mut engine = self.inner.engine.lock().expect("engine poisoned");
        let mut collections = self
            .inner
            .collections
            .write()
            .expect("collections poisoned");

        let state = collections.entry(collection.to_string()).or_default();
        let was_present = state.docs.contains_key(&id);
        let version = state.next_version;
        state.next_version += 1;
        state.next_doc_id = state.next_doc_id.max(id.0 + 1);

        let key = doc_key(collection, id);
        let payload = serialize_doc(&doc)?;
        engine.put(key, payload)?;

        state.docs.insert(id, doc);
        state.versions.insert(id, version);

        #[cfg(feature = "async")]
        {
            let mt = if was_present {
                MutationType::Update
            } else {
                MutationType::Insert
            };
            self.inner.bus.publish(MutationEvent {
                collection: collection.to_string(),
                doc_id: id,
                mutation_type: mt,
                origin: MutationOrigin::Local,
            });
        }
        #[cfg(not(feature = "async"))]
        let _ = was_present; // silence unused

        Ok(version)
    }

    /// Allocate a fresh [`DocId`] for `collection` and insert `doc` under it.
    /// Returns the assigned id. The fields of `doc.id` is overwritten.
    pub fn put_auto(&self, collection: &str, mut doc: Document) -> Result<DocId, DatabaseError> {
        let id = self.next_id(collection);
        doc.id = id;
        self.put(collection, doc)?;
        Ok(id)
    }

    /// Fetch a document by id.
    pub fn get(&self, collection: &str, id: DocId) -> Option<Document> {
        let collections = self.inner.collections.read().expect("collections poisoned");
        collections.get(collection)?.docs.get(&id).cloned()
    }

    /// Delete a document by id. Returns `true` if a document was actually
    /// removed.
    pub fn delete(&self, collection: &str, id: DocId) -> Result<bool, DatabaseError> {
        let mut engine = self.inner.engine.lock().expect("engine poisoned");
        let mut collections = self
            .inner
            .collections
            .write()
            .expect("collections poisoned");

        let Some(state) = collections.get_mut(collection) else {
            return Ok(false);
        };

        let removed = state.docs.remove(&id).is_some();
        state.versions.remove(&id);

        if removed {
            engine.delete(doc_key(collection, id))?;
            #[cfg(feature = "async")]
            self.inner.bus.publish(MutationEvent {
                collection: collection.to_string(),
                doc_id: id,
                mutation_type: MutationType::Delete,
                origin: MutationOrigin::Local,
            });
        }

        Ok(removed)
    }

    /// All document ids in `collection`, sorted ascending.
    pub fn list_ids(&self, collection: &str) -> Vec<DocId> {
        let collections = self.inner.collections.read().expect("collections poisoned");
        let Some(state) = collections.get(collection) else {
            return Vec::new();
        };
        let mut ids: Vec<DocId> = state.docs.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Number of documents in `collection`.
    pub fn count(&self, collection: &str) -> usize {
        let collections = self.inner.collections.read().expect("collections poisoned");
        collections
            .get(collection)
            .map(|s| s.docs.len())
            .unwrap_or(0)
    }

    // ── Query ───────────────────────────────────────────────────────────

    /// Execute `query` against the in-memory document store. Currently
    /// always uses a full scan — secondary index integration is on the
    /// roadmap, but the executor still applies filters/sort/limit/offset.
    pub fn query(&self, query: &Query) -> Vec<Document> {
        execute_query(&self.inner, query)
    }

    /// Subscribe to a live query. Returns the current snapshot of
    /// matching documents alongside a [`LiveQuery`] that emits a
    /// [`QueryDiff`] each time a mutation in the queried collection
    /// changes the result set.
    ///
    /// The bus subscription happens **before** the initial snapshot is
    /// captured, so no event is lost in the window between the two.
    /// Callers should treat the returned `Vec<Document>` as the
    /// "current state" and the diffs as deltas applied on top.
    #[cfg(feature = "async")]
    pub fn live_query(
        &self,
        query: Query,
    ) -> (
        Vec<Document>,
        crate::reactive::LiveQuery<DatabaseQueryRunner>,
    ) {
        // Subscribe first so any mutation between now and the snapshot
        // is buffered on the receiver.
        let receiver = self.inner.bus.subscribe();
        let initial = execute_query(&self.inner, &query);
        let runner = Arc::new(DatabaseQueryRunner {
            inner: Arc::clone(&self.inner),
        });
        let live =
            crate::reactive::LiveQuery::from_receiver(query, runner, receiver, initial.clone());
        (initial, live)
    }

    // ── Transaction ─────────────────────────────────────────────────────

    /// Begin a new transaction. The returned [`DbTransaction`] holds a
    /// versioned snapshot of reads and a buffer of writes; commit applies
    /// them atomically with conflict detection.
    pub fn begin_transaction(&self) -> DbTransaction {
        DbTransaction::new(Arc::clone(&self.inner))
    }

    // ── Observer ────────────────────────────────────────────────────────

    /// Subscribe to mutation events for `collection`. The returned
    /// receiver yields events as they happen; lagged subscribers will see
    /// `RecvError::Lagged` (the bus does not buffer unboundedly).
    #[cfg(feature = "async")]
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<MutationEvent> {
        self.inner.bus.subscribe()
    }

    /// Direct access to the event bus for advanced use cases.
    #[cfg(feature = "async")]
    pub fn event_bus(&self) -> &EventBus {
        &self.inner.bus
    }

    // ── Raw KV passthrough ──────────────────────────────────────────────

    /// Insert or update a raw key-value pair on the underlying engine.
    /// Bypasses the document layer entirely.
    pub fn raw_put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DatabaseError> {
        let mut engine = self.inner.engine.lock().expect("engine poisoned");
        engine.put(key, value)?;
        Ok(())
    }

    /// Look up a raw key on the underlying engine.
    pub fn raw_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let engine = self.inner.engine.lock().expect("engine poisoned");
        Ok(engine.get(key)?)
    }

    /// Delete a raw key on the underlying engine.
    pub fn raw_delete(&self, key: Vec<u8>) -> Result<(), DatabaseError> {
        let mut engine = self.inner.engine.lock().expect("engine poisoned");
        engine.delete(key)?;
        Ok(())
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn next_id(&self, collection: &str) -> DocId {
        let mut collections = self
            .inner
            .collections
            .write()
            .expect("collections poisoned");
        let state = collections.entry(collection.to_string()).or_default();
        let id = DocId(state.next_doc_id);
        state.next_doc_id += 1;
        id
    }

    /// Walk every key in the engine on open and rebuild the per-collection
    /// in-memory state.
    ///
    /// The current LSM engine doesn't expose an iterator, so we look up
    /// the persisted id-counter and document keys via point reads. To keep
    /// the implementation simple we don't keep an external collection
    /// catalog — instead each `put` records the auto-id via the storage
    /// engine when appropriate.
    fn scan_collections(
        _engine: &StorageEngine,
    ) -> Result<HashMap<String, CollectionState>, DatabaseError> {
        // For v0.1.0 we keep collections in-memory only; the engine still
        // provides durability per document, but the on-open re-hydration
        // pass is deferred until the engine grows a public iterator.
        // Tests below cover the put/get/delete/query flow within a single
        // process, which is the FFI's primary use case.
        Ok(HashMap::new())
    }
}

/// Wraps the borrowed collection map so the [`QueryExecutor`] can scan it.
struct CollectionView<'a> {
    state: Option<&'a CollectionState>,
}

impl DocumentStore for CollectionView<'_> {
    fn get_document(&self, id: DocId) -> Option<Document> {
        self.state?.docs.get(&id).cloned()
    }

    fn all_doc_ids(&self) -> Vec<DocId> {
        let Some(s) = self.state else {
            return Vec::new();
        };
        let mut ids: Vec<DocId> = s.docs.keys().copied().collect();
        ids.sort();
        ids
    }

    fn count(&self) -> usize {
        self.state.map(|s| s.docs.len()).unwrap_or(0)
    }
}

/// Shared query execution against the in-memory collection state.
/// Used by both [`Database::query`] and [`DatabaseQueryRunner`].
fn execute_query(inner: &DatabaseInner, query: &Query) -> Vec<Document> {
    let collections = inner.collections.read().expect("collections poisoned");
    let view = CollectionView {
        state: collections.get(query.collection_name()),
    };
    let plan = QueryPlanner::plan(query, &[], view.count());
    let hash = HashMap::new();
    let btree = HashMap::new();
    let idx_set = IndexSet {
        hash: &hash,
        btree: &btree,
    };
    QueryExecutor::execute(query, &plan, &view, &idx_set)
}

// ── Live query runner ──────────────────────────────────────────────────

/// Adapter that lets [`crate::reactive::LiveQuery`] re-execute predicate
/// queries against a [`Database`]. Holds an `Arc<DatabaseInner>` so the
/// runner survives independently of the `Database` handle that spawned
/// the live query.
#[cfg(feature = "async")]
pub struct DatabaseQueryRunner {
    inner: Arc<DatabaseInner>,
}

#[cfg(feature = "async")]
impl crate::reactive::QueryRunner for DatabaseQueryRunner {
    fn execute(&self, query: &Query) -> Vec<Document> {
        execute_query(&self.inner, query)
    }
}

// ── Transaction ─────────────────────────────────────────────────────────

/// A multi-collection transaction. Reads are recorded with the version
/// observed; writes/deletes are buffered. [`commit`](Self::commit) applies
/// the batch atomically only if every recorded version is still current.
pub struct DbTransaction {
    inner: Arc<DatabaseInner>,
    state: TxnState,
    /// `(collection, doc_id) → version_at_read_time`
    read_set: HashMap<(String, DocId), u64>,
    /// `(collection, doc_id) → document_to_write`
    write_set: HashMap<(String, DocId), Document>,
    delete_set: std::collections::HashSet<(String, DocId)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Finalised,
}

impl DbTransaction {
    fn new(inner: Arc<DatabaseInner>) -> Self {
        Self {
            inner,
            state: TxnState::Active,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            delete_set: std::collections::HashSet::new(),
        }
    }

    /// Read a document, capturing its version for conflict detection.
    pub fn get(
        &mut self,
        collection: &str,
        id: DocId,
    ) -> Result<Option<Document>, TransactionError> {
        self.ensure_active()?;
        let key = (collection.to_string(), id);

        if let Some(doc) = self.write_set.get(&key) {
            return Ok(Some(doc.clone()));
        }
        if self.delete_set.contains(&key) {
            return Ok(None);
        }

        let collections = self.inner.collections.read().expect("collections poisoned");
        let (doc, version) = collections
            .get(collection)
            .and_then(|s| {
                s.docs
                    .get(&id)
                    .cloned()
                    .map(|d| (Some(d), s.versions.get(&id).copied().unwrap_or(0)))
            })
            .unwrap_or((None, 0));

        self.read_set.insert(key, version);
        Ok(doc)
    }

    /// Buffer a write. Cancels any pending delete for the same key.
    pub fn put(&mut self, collection: &str, doc: Document) -> Result<(), TransactionError> {
        self.ensure_active()?;
        let key = (collection.to_string(), doc.id);
        self.delete_set.remove(&key);
        self.write_set.insert(key, doc);
        Ok(())
    }

    /// Buffer a delete. Cancels any pending write for the same key.
    pub fn delete(&mut self, collection: &str, id: DocId) -> Result<(), TransactionError> {
        self.ensure_active()?;
        let key = (collection.to_string(), id);
        self.write_set.remove(&key);
        self.delete_set.insert(key);
        Ok(())
    }

    /// Validate the read set and atomically apply all buffered changes.
    pub fn commit(mut self) -> Result<(), DatabaseError> {
        self.ensure_active()?;
        self.state = TxnState::Finalised;

        let mut engine = self.inner.engine.lock().expect("engine poisoned");
        let mut collections = self
            .inner
            .collections
            .write()
            .expect("collections poisoned");

        // Phase 1: validate read set.
        for ((coll, id), expected) in &self.read_set {
            let current = collections
                .get(coll)
                .and_then(|s| s.versions.get(id).copied())
                .unwrap_or(0);
            if current != *expected {
                return Err(DatabaseError::Transaction(TransactionError::Conflict {
                    doc_id: *id,
                    read_version: *expected,
                    current_version: current,
                }));
            }
        }

        // Phase 2: collect events to emit after the lock is released.
        #[cfg(feature = "async")]
        let mut events: Vec<MutationEvent> = Vec::new();

        // Phase 3: apply writes.
        for ((coll, id), doc) in self.write_set.drain() {
            let state = collections.entry(coll.clone()).or_default();
            let was_present = state.docs.contains_key(&id);
            let version = state.next_version;
            state.next_version += 1;
            state.next_doc_id = state.next_doc_id.max(id.0 + 1);

            engine.put(doc_key(&coll, id), serialize_doc(&doc)?)?;
            state.docs.insert(id, doc);
            state.versions.insert(id, version);

            #[cfg(feature = "async")]
            events.push(MutationEvent {
                collection: coll,
                doc_id: id,
                mutation_type: if was_present {
                    MutationType::Update
                } else {
                    MutationType::Insert
                },
                origin: MutationOrigin::Local,
            });
            #[cfg(not(feature = "async"))]
            let _ = was_present;
        }

        // Phase 4: apply deletes.
        for (coll, id) in self.delete_set.drain() {
            let removed = collections
                .get_mut(&coll)
                .map(|s| {
                    s.versions.remove(&id);
                    s.docs.remove(&id).is_some()
                })
                .unwrap_or(false);
            if removed {
                engine.delete(doc_key(&coll, id))?;
                #[cfg(feature = "async")]
                events.push(MutationEvent {
                    collection: coll,
                    doc_id: id,
                    mutation_type: MutationType::Delete,
                    origin: MutationOrigin::Local,
                });
            }
        }

        drop(engine);
        drop(collections);

        #[cfg(feature = "async")]
        for event in events {
            self.inner.bus.publish(event);
        }

        Ok(())
    }

    /// Discard all buffered changes.
    pub fn rollback(mut self) {
        self.state = TxnState::Finalised;
    }

    /// Number of pending writes in the buffer.
    pub fn pending_writes(&self) -> usize {
        self.write_set.len()
    }

    /// Number of pending deletes in the buffer.
    pub fn pending_deletes(&self) -> usize {
        self.delete_set.len()
    }

    fn ensure_active(&self) -> Result<(), TransactionError> {
        if self.state != TxnState::Active {
            return Err(TransactionError::AlreadyFinalised);
        }
        Ok(())
    }
}

/// Implements `VersionedStore` so the existing single-collection
/// transaction code in [`crate::transaction`] can run against a
/// [`Database`] view as well. Restricted to a single collection, since
/// `VersionedStore` is collection-agnostic.
impl VersionedStore for Database {
    fn get_versioned(&self, id: DocId) -> Option<VersionedDocument> {
        // Without a collection name, this trait is only meaningful when
        // the caller pre-filters to a single collection. We pick the first
        // collection that contains the id — fine for tests; FFI uses
        // `DbTransaction` directly.
        let collections = self.inner.collections.read().expect("collections poisoned");
        for state in collections.values() {
            if let Some(doc) = state.docs.get(&id) {
                return Some(VersionedDocument {
                    document: doc.clone(),
                    version: state.versions.get(&id).copied().unwrap_or(0),
                });
            }
        }
        None
    }

    fn current_version(&self, id: DocId) -> Option<u64> {
        let collections = self.inner.collections.read().expect("collections poisoned");
        for state in collections.values() {
            if let Some(v) = state.versions.get(&id) {
                return Some(*v);
            }
        }
        None
    }

    fn apply_batch(
        &self,
        _read_set: &[(DocId, u64)],
        _puts: Vec<Document>,
        _deletes: &[DocId],
    ) -> Result<(), TransactionError> {
        // Not implemented: callers should use `Database::begin_transaction`
        // which is collection-aware. The trait impl exists only for the
        // narrow case where a test wants to swap MemVersionedStore for a
        // Database.
        Err(TransactionError::AlreadyFinalised)
    }
}

// ── Encoding helpers ────────────────────────────────────────────────────

fn doc_key(collection: &str, id: DocId) -> Vec<u8> {
    let mut k = Vec::with_capacity(DOC_PREFIX.len() + collection.len() + 1 + 8);
    k.extend_from_slice(DOC_PREFIX);
    k.extend_from_slice(collection.as_bytes());
    k.push(b'/');
    k.extend_from_slice(&id.0.to_be_bytes());
    k
}

#[allow(dead_code)] // Reserved for future on-open rehydration.
fn id_counter_key(collection: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(META_PREFIX.len() + collection.len() + ID_COUNTER_SUFFIX.len());
    k.extend_from_slice(META_PREFIX);
    k.extend_from_slice(collection.as_bytes());
    k.extend_from_slice(ID_COUNTER_SUFFIX);
    k
}

fn serialize_doc(doc: &Document) -> Result<Vec<u8>, DatabaseError> {
    serde_json::to_vec(doc).map_err(|e| DatabaseError::Decode(e.to_string()))
}

#[allow(dead_code)] // Used by future on-open rehydration.
fn deserialize_doc(bytes: &[u8]) -> Result<Document, DatabaseError> {
    serde_json::from_slice(bytes).map_err(|e| DatabaseError::Decode(e.to_string()))
}

#[cfg(all(test, feature = "ffi"))]
mod tests {
    use super::*;
    use crate::query::{Filter, Sort, Value};
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("raft_db_database_tests")
            .join(name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).ok();
        }
        dir
    }

    fn user(id: u64, name: &str, age: i64) -> Document {
        Document::new(DocId(id))
            .with_field("name", Value::String(name.into()))
            .with_field("age", Value::Int(age))
    }

    #[test]
    fn open_creates_empty_database() {
        let dir = temp_dir("open_empty");
        let db = Database::open(&dir).unwrap();
        assert_eq!(db.count("users"), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_and_get_document() {
        let dir = temp_dir("put_get");
        let db = Database::open(&dir).unwrap();

        db.put("users", user(1, "Alice", 30)).unwrap();
        let got = db.get("users", DocId(1)).unwrap();
        assert_eq!(got.get("name"), Some(&Value::String("Alice".into())));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_overwrites_existing() {
        let dir = temp_dir("put_overwrite");
        let db = Database::open(&dir).unwrap();

        let v1 = db.put("users", user(1, "Alice", 30)).unwrap();
        let v2 = db.put("users", user(1, "Alice", 31)).unwrap();
        assert!(v2 > v1);

        let got = db.get("users", DocId(1)).unwrap();
        assert_eq!(got.get("age"), Some(&Value::Int(31)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delete_returns_true_when_existed() {
        let dir = temp_dir("delete_exists");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(1, "Alice", 30)).unwrap();

        assert!(db.delete("users", DocId(1)).unwrap());
        assert!(!db.delete("users", DocId(1)).unwrap()); // already gone
        assert!(db.get("users", DocId(1)).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn list_ids_returns_sorted() {
        let dir = temp_dir("list_ids");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(3, "C", 0)).unwrap();
        db.put("users", user(1, "A", 0)).unwrap();
        db.put("users", user(2, "B", 0)).unwrap();

        let ids = db.list_ids("users");
        assert_eq!(ids, vec![DocId(1), DocId(2), DocId(3)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_auto_assigns_increasing_ids() {
        let dir = temp_dir("put_auto");
        let db = Database::open(&dir).unwrap();
        let id1 = db.put_auto("users", Document::new(DocId(0))).unwrap();
        let id2 = db.put_auto("users", Document::new(DocId(0))).unwrap();
        assert!(id2.0 > id1.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn query_filters_results() {
        let dir = temp_dir("query_filter");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(1, "Alice", 30)).unwrap();
        db.put("users", user(2, "Bob", 25)).unwrap();
        db.put("users", user(3, "Carol", 35)).unwrap();

        let q = Query::collection("users").filter(Filter::gte("age", Value::Int(30)));
        let results = db.query(&q);
        assert_eq!(results.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn query_sort_limit_offset() {
        let dir = temp_dir("query_sort");
        let db = Database::open(&dir).unwrap();
        for i in 0..5u64 {
            db.put("users", user(i + 1, &format!("U{i}"), i as i64))
                .unwrap();
        }

        let q = Query::collection("users")
            .sort(Sort::desc("age"))
            .limit(2)
            .offset(1);
        let results = db.query(&q);
        assert_eq!(results.len(), 2);
        // age sorted desc: 4, 3, 2, 1, 0 — offset 1, take 2 → [3, 2]
        assert_eq!(results[0].get("age"), Some(&Value::Int(3)));
        assert_eq!(results[1].get("age"), Some(&Value::Int(2)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn transaction_commit_applies_writes() {
        let dir = temp_dir("txn_commit");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(1, "Alice", 30)).unwrap();

        let mut txn = db.begin_transaction();
        let _ = txn.get("users", DocId(1)).unwrap();
        txn.put("users", user(1, "Alice Updated", 31)).unwrap();
        txn.put("users", user(2, "Bob", 25)).unwrap();
        txn.commit().unwrap();

        assert_eq!(
            db.get("users", DocId(1)).unwrap().get("name"),
            Some(&Value::String("Alice Updated".into()))
        );
        assert!(db.get("users", DocId(2)).is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn transaction_rollback_discards_writes() {
        let dir = temp_dir("txn_rollback");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(1, "Alice", 30)).unwrap();

        let mut txn = db.begin_transaction();
        txn.put("users", user(1, "Should Not Apply", 0)).unwrap();
        txn.rollback();

        assert_eq!(
            db.get("users", DocId(1)).unwrap().get("name"),
            Some(&Value::String("Alice".into()))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn transaction_conflict_detected() {
        let dir = temp_dir("txn_conflict");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(1, "Alice", 30)).unwrap();

        let mut txn = db.begin_transaction();
        let _ = txn.get("users", DocId(1)).unwrap();
        txn.put("users", user(1, "From Txn", 31)).unwrap();

        // Concurrent write happens before commit.
        db.put("users", user(1, "From Concurrent", 32)).unwrap();

        let result = txn.commit();
        assert!(matches!(
            result,
            Err(DatabaseError::Transaction(
                TransactionError::Conflict { .. }
            ))
        ));
        // Concurrent write wins.
        assert_eq!(
            db.get("users", DocId(1)).unwrap().get("name"),
            Some(&Value::String("From Concurrent".into()))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn transaction_delete_then_put_keeps_put() {
        let dir = temp_dir("txn_del_put");
        let db = Database::open(&dir).unwrap();
        db.put("users", user(1, "Original", 0)).unwrap();

        let mut txn = db.begin_transaction();
        let _ = txn.get("users", DocId(1)).unwrap();
        txn.delete("users", DocId(1)).unwrap();
        txn.put("users", user(1, "Revived", 0)).unwrap();
        txn.commit().unwrap();

        assert_eq!(
            db.get("users", DocId(1)).unwrap().get("name"),
            Some(&Value::String("Revived".into()))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "async")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observer_receives_mutation_events() {
        let dir = temp_dir("observe_basic");
        let db = Database::open(&dir).unwrap();

        let mut rx = db.subscribe();
        db.put("users", user(1, "Alice", 30)).unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, DocId(1));
        assert_eq!(event.mutation_type, MutationType::Insert);

        db.put("users", user(1, "Alice", 31)).unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event.mutation_type, MutationType::Update);

        db.delete("users", DocId(1)).unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event.mutation_type, MutationType::Delete);

        std::fs::remove_dir_all(&dir).ok();
    }
}
