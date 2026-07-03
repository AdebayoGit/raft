//! Query executor — runs a [`QueryPlan`] against a [`DocumentStore`].
//!
//! Execution flow:
//! 1. Fetch candidate documents lazily (via index or full scan).
//! 2. Apply remaining filter predicates in memory.
//! 3. Apply sort / offset / limit:
//!    - unsorted + limit: stream and stop as soon as `offset + limit`
//!      matches have been seen — documents past the cut are never fetched;
//!    - sorted + limit: bounded top-k selection keeps only the best
//!      `offset + limit` candidates in memory instead of sorting the full
//!      result set;
//!    - sorted, no limit: full stable sort.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::index::{BTreeIndex, DocId, HashIndex, Index};

use super::document::{Document, DocumentStore, Value};
use super::planner::{QueryPlan, ScanStrategy};
use super::sort::SortDirection;
use super::Query;

/// Holds index references the executor can use.
pub struct IndexSet<'a> {
    pub hash: &'a std::collections::HashMap<String, HashIndex>,
    pub btree: &'a std::collections::HashMap<String, BTreeIndex>,
}

/// Stateless query executor.
pub struct QueryExecutor;

impl QueryExecutor {
    /// Execute `query` according to `plan` against `store`, using `indexes`
    /// for index-assisted scans.
    pub fn execute(
        query: &Query,
        plan: &QueryPlan,
        store: &dyn DocumentStore,
        indexes: &IndexSet<'_>,
    ) -> Vec<Document> {
        // Step 1: Fetch candidate doc IDs.
        let candidate_ids: Vec<DocId> = match &plan.strategy {
            ScanStrategy::FullScan => store.all_doc_ids(),

            ScanStrategy::HashLookup { field, key } => {
                if let Some(idx) = indexes.hash.get(field) {
                    idx.lookup(key)
                } else {
                    store.all_doc_ids()
                }
            }

            ScanStrategy::HashUnion { field, keys } => {
                if let Some(idx) = indexes.hash.get(field) {
                    let mut ids: Vec<DocId> = keys.iter().flat_map(|k| idx.lookup(k)).collect();
                    // Different keys can never share an id, but sorting
                    // restores the ascending-id order the top-k tie-break
                    // below relies on; dedup guards against overlap if a
                    // planner ever emits duplicate keys.
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                } else {
                    store.all_doc_ids()
                }
            }

            ScanStrategy::BTreeRange {
                field,
                start,
                start_inclusive,
                end,
                end_inclusive,
            } => {
                if let Some(idx) = indexes.btree.get(field) {
                    Self::btree_range_lookup(idx, start, *start_inclusive, end, *end_inclusive)
                } else {
                    store.all_doc_ids()
                }
            }
        };

        // Step 2: Lazily fetch documents and apply the filter. Nothing is
        // materialised until one of the terminal branches below consumes
        // the iterator.
        let matches = candidate_ids
            .into_iter()
            .filter_map(|id| store.get_document(id))
            .filter(|doc| match query.get_filter() {
                Some(filter) => filter.matches(&|field_name: &str| doc.get(field_name).cloned()),
                None => true,
            });

        let offset = query.get_offset().unwrap_or(0);

        match (query.get_sort(), query.get_limit()) {
            // Unsorted: stream in candidate order with early exit — the
            // iterator stops fetching documents once `offset + limit`
            // matches have been produced.
            (None, Some(limit)) => matches.skip(offset).take(limit).collect(),
            (None, None) => matches.skip(offset).collect(),

            // Sorted + limited: bounded top-k selection. Only the best
            // `offset + limit` candidates are retained, so memory stays
            // O(k) instead of O(result set).
            (Some(sort), Some(limit)) => {
                let keep = offset.saturating_add(limit);
                if keep == 0 {
                    return Vec::new();
                }
                let desc = sort.direction == SortDirection::Descending;
                let mut heap: BinaryHeap<Candidate> = BinaryHeap::with_capacity(keep + 1);
                for doc in matches {
                    heap.push(Candidate {
                        key: doc.get(&sort.field).cloned(),
                        desc,
                        doc,
                    });
                    if heap.len() > keep {
                        heap.pop(); // discard the current worst
                    }
                }
                heap.into_sorted_vec()
                    .into_iter()
                    .skip(offset)
                    .map(|c| c.doc)
                    .collect()
            }

            // Sorted, unlimited: full stable sort.
            (Some(sort), None) => {
                let mut results: Vec<Document> = matches.collect();
                let desc = sort.direction == SortDirection::Descending;
                results.sort_by(|a, b| directed_cmp(a.get(&sort.field), b.get(&sort.field), desc));
                if offset > 0 {
                    results.drain(..offset.min(results.len()));
                }
                results
            }
        }
    }

    fn btree_range_lookup(
        idx: &BTreeIndex,
        start: &Option<Vec<u8>>,
        start_inclusive: bool,
        end: &Option<Vec<u8>>,
        end_inclusive: bool,
    ) -> Vec<DocId> {
        use std::ops::Bound;

        let lo: Bound<Vec<u8>> = match start {
            Some(k) if start_inclusive => Bound::Included(k.clone()),
            Some(k) => Bound::Excluded(k.clone()),
            None => Bound::Unbounded,
        };
        let hi: Bound<Vec<u8>> = match end {
            Some(k) if end_inclusive => Bound::Included(k.clone()),
            Some(k) => Bound::Excluded(k.clone()),
            None => Bound::Unbounded,
        };

        idx.range((lo, hi))
    }
}

/// Compare two optional field values with the query's sort semantics:
/// missing fields sort last, incomparable values compare equal, and
/// `desc` reverses the value ordering (but not the missing-field rule's
/// relative outcome — reversal applies to the whole comparison, exactly
/// as the pre-existing sort did).
fn directed_cmp(a: Option<&Value>, b: Option<&Value>, desc: bool) -> Ordering {
    let ord = match (a, b) {
        (Some(a), Some(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };
    if desc {
        ord.reverse()
    } else {
        ord
    }
}

/// Heap entry for bounded top-k selection.
///
/// Orders by the (direction-applied) sort key, breaking ties by ascending
/// [`DocId`] — the same tie order a stable sort produces, since candidate
/// ids arrive sorted ascending.
struct Candidate {
    key: Option<Value>,
    desc: bool,
    doc: Document,
}

impl Candidate {
    fn total_cmp(&self, other: &Self) -> Ordering {
        directed_cmp(self.key.as_ref(), other.key.as_ref(), self.desc)
            .then(self.doc.id.cmp(&other.doc.id))
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.total_cmp(other) == Ordering::Equal
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_cmp(other)
    }
}

// ── In-memory test store ──────────────────────────────────────────────

#[cfg(test)]
mod test_store {
    use super::*;
    use std::collections::HashMap;

    /// Simple in-memory document store for testing.
    pub struct MemDocStore {
        pub docs: HashMap<DocId, Document>,
    }

    impl MemDocStore {
        pub fn new() -> Self {
            Self {
                docs: HashMap::new(),
            }
        }

        pub fn insert(&mut self, doc: Document) {
            self.docs.insert(doc.id, doc);
        }
    }

    impl DocumentStore for MemDocStore {
        fn get_document(&self, id: DocId) -> Option<Document> {
            self.docs.get(&id).cloned()
        }

        fn all_doc_ids(&self) -> Vec<DocId> {
            let mut ids: Vec<DocId> = self.docs.keys().copied().collect();
            ids.sort();
            ids
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_store::MemDocStore;
    use super::*;
    use crate::index::{BTreeIndex, HashIndex, Index};
    use crate::query::planner::{IndexInfo, IndexKind, QueryPlanner};
    use crate::query::sort::Sort;
    use crate::query::{Filter, Value};
    use std::collections::HashMap;

    fn make_user(id: u64, name: &str, age: i64, active: bool) -> Document {
        Document::new(DocId(id))
            .with_field("name", Value::String(name.into()))
            .with_field("age", Value::Int(age))
            .with_field("active", Value::Bool(active))
    }

    fn sample_store() -> MemDocStore {
        let mut store = MemDocStore::new();
        store.insert(make_user(1, "Alice", 30, true));
        store.insert(make_user(2, "Bob", 25, false));
        store.insert(make_user(3, "Charlie", 35, true));
        store.insert(make_user(4, "Diana", 28, true));
        store.insert(make_user(5, "Eve", 22, false));
        store
    }

    fn build_hash_index(store: &MemDocStore, field: &str) -> HashIndex {
        let mut idx = HashIndex::new();
        for doc in store.docs.values() {
            if let Some(val) = doc.get(field) {
                idx.insert(&val.to_index_bytes(), doc.id);
            }
        }
        idx
    }

    fn build_btree_index(store: &MemDocStore, field: &str) -> BTreeIndex {
        let mut idx = BTreeIndex::new();
        for doc in store.docs.values() {
            if let Some(val) = doc.get(field) {
                idx.insert(&val.to_index_bytes(), doc.id);
            }
        }
        idx
    }

    fn empty_indexes() -> (HashMap<String, HashIndex>, HashMap<String, BTreeIndex>) {
        (HashMap::new(), HashMap::new())
    }

    // ── Full scan ──

    #[test]
    fn full_scan_no_filter() {
        let store = sample_store();
        let q = Query::collection("users");
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn full_scan_with_filter() {
        let store = sample_store();
        let q = Query::collection("users").filter(Filter::eq("active", Value::Bool(true)));
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 3);
        assert!(results
            .iter()
            .all(|d| d.get("active") == Some(&Value::Bool(true))));
    }

    // ── Hash union (same-field Or of equalities, Q6c) ──

    #[test]
    fn hash_union_or_lookup() {
        let store = sample_store();
        let hash_idx = build_hash_index(&store, "name");
        let mut hash_map = HashMap::new();
        hash_map.insert("name".to_string(), hash_idx);

        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("name", Value::String("Alice".into())),
            Filter::eq("name", Value::String("Eve".into())),
        ]));
        let indexes = vec![IndexInfo {
            field: "name".into(),
            kind: IndexKind::Hash,
            entry_count: 5,
        }];
        let plan = QueryPlanner::plan(&q, &indexes, store.count());
        assert!(matches!(plan.strategy, ScanStrategy::HashUnion { .. }));

        let btree_map = HashMap::new();
        let idx_set = IndexSet {
            hash: &hash_map,
            btree: &btree_map,
        };
        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        let mut names: Vec<_> = results
            .iter()
            .filter_map(|d| d.get("name").cloned())
            .collect();
        names.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            names,
            vec![Value::String("Alice".into()), Value::String("Eve".into()),]
        );
    }

    #[test]
    fn hash_union_duplicate_keys_dedup() {
        let store = sample_store();
        let hash_idx = build_hash_index(&store, "name");
        let mut hash_map = HashMap::new();
        hash_map.insert("name".to_string(), hash_idx);
        let btree_map = HashMap::new();
        let idx_set = IndexSet {
            hash: &hash_map,
            btree: &btree_map,
        };

        // Same key twice — the union must not return the doc twice.
        let key = Value::String("Alice".into()).to_index_bytes();
        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("name", Value::String("Alice".into())),
            Filter::eq("name", Value::String("Alice".into())),
        ]));
        let plan = QueryPlan {
            strategy: ScanStrategy::HashUnion {
                field: "name".into(),
                keys: vec![key.clone(), key],
            },
            estimated_cost: 2,
        };
        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocId(1));
    }

    #[test]
    fn hash_union_missing_index_falls_back_to_scan() {
        let store = sample_store();
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("name", Value::String("Bob".into())),
            Filter::eq("name", Value::String("Diana".into())),
        ]));
        let plan = QueryPlan {
            strategy: ScanStrategy::HashUnion {
                field: "name".into(),
                keys: vec![
                    Value::String("Bob".into()).to_index_bytes(),
                    Value::String("Diana".into()).to_index_bytes(),
                ],
            },
            estimated_cost: 2,
        };
        // No hash index registered — falls back to scanning everything,
        // then the filter narrows the results.
        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 2);
    }

    // ── Hash index ──

    #[test]
    fn hash_index_eq_lookup() {
        let store = sample_store();
        let hash_idx = build_hash_index(&store, "name");
        let mut hash_map = HashMap::new();
        hash_map.insert("name".to_string(), hash_idx);

        let q =
            Query::collection("users").filter(Filter::eq("name", Value::String("Alice".into())));
        let indexes = vec![IndexInfo {
            field: "name".into(),
            kind: IndexKind::Hash,
            entry_count: 5,
        }];
        let plan = QueryPlanner::plan(&q, &indexes, store.count());
        assert!(matches!(plan.strategy, ScanStrategy::HashLookup { .. }));

        let btree_map = HashMap::new();
        let idx_set = IndexSet {
            hash: &hash_map,
            btree: &btree_map,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocId(1));
    }

    // ── BTree index ──

    #[test]
    fn btree_index_range_query() {
        let store = sample_store();
        let btree_idx = build_btree_index(&store, "age");
        let mut btree_map = HashMap::new();
        btree_map.insert("age".to_string(), btree_idx);

        let q = Query::collection("users").filter(Filter::gte("age", Value::Int(28)));
        let indexes = vec![IndexInfo {
            field: "age".into(),
            kind: IndexKind::BTree,
            entry_count: 5,
        }];
        let plan = QueryPlanner::plan(&q, &indexes, store.count());
        assert!(matches!(plan.strategy, ScanStrategy::BTreeRange { .. }));

        let hash_map = HashMap::new();
        let idx_set = IndexSet {
            hash: &hash_map,
            btree: &btree_map,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        // age >= 28: Alice(30), Charlie(35), Diana(28)
        assert_eq!(results.len(), 3);
        for doc in &results {
            if let Some(Value::Int(age)) = doc.get("age") {
                assert!(*age >= 28);
            }
        }
    }

    #[test]
    fn btree_index_eq_lookup() {
        let store = sample_store();
        let btree_idx = build_btree_index(&store, "age");
        let mut btree_map = HashMap::new();
        btree_map.insert("age".to_string(), btree_idx);

        let q = Query::collection("users").filter(Filter::eq("age", Value::Int(25)));
        let indexes = vec![IndexInfo {
            field: "age".into(),
            kind: IndexKind::BTree,
            entry_count: 5,
        }];
        let plan = QueryPlanner::plan(&q, &indexes, store.count());

        let hash_map = HashMap::new();
        let idx_set = IndexSet {
            hash: &hash_map,
            btree: &btree_map,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::String("Bob".into())));
    }

    // ── Sort ──

    #[test]
    fn sort_ascending() {
        let store = sample_store();
        let q = Query::collection("users")
            .filter(Filter::eq("active", Value::Bool(true)))
            .sort(Sort::asc("age"));
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        let ages: Vec<i64> = results
            .iter()
            .filter_map(|d| match d.get("age") {
                Some(Value::Int(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(ages, vec![28, 30, 35]);
    }

    #[test]
    fn sort_descending() {
        let store = sample_store();
        let q = Query::collection("users").sort(Sort::desc("age"));
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        let ages: Vec<i64> = results
            .iter()
            .filter_map(|d| match d.get("age") {
                Some(Value::Int(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(ages, vec![35, 30, 28, 25, 22]);
    }

    // ── Limit / Offset ──

    #[test]
    fn limit() {
        let store = sample_store();
        let q = Query::collection("users").sort(Sort::asc("age")).limit(3);
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 3);
        let ages: Vec<i64> = results
            .iter()
            .filter_map(|d| match d.get("age") {
                Some(Value::Int(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(ages, vec![22, 25, 28]);
    }

    #[test]
    fn offset_and_limit() {
        let store = sample_store();
        let q = Query::collection("users")
            .sort(Sort::asc("age"))
            .offset(1)
            .limit(2);
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 2);
        let ages: Vec<i64> = results
            .iter()
            .filter_map(|d| match d.get("age") {
                Some(Value::Int(n)) => Some(*n),
                _ => None,
            })
            .collect();
        // Sorted: [22, 25, 28, 30, 35], skip 1, take 2 → [25, 28]
        assert_eq!(ages, vec![25, 28]);
    }

    #[test]
    fn offset_beyond_results() {
        let store = sample_store();
        let q = Query::collection("users").offset(100);
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert!(results.is_empty());
    }

    // ── Combined ──

    #[test]
    fn filter_sort_limit_offset_combined() {
        let store = sample_store();
        // active=true → Alice(30), Charlie(35), Diana(28)
        // sorted by age asc → Diana(28), Alice(30), Charlie(35)
        // offset 1 → Alice(30), Charlie(35)
        // limit 1 → Alice(30)
        let q = Query::collection("users")
            .filter(Filter::eq("active", Value::Bool(true)))
            .sort(Sort::asc("age"))
            .offset(1)
            .limit(1);
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn index_assisted_with_residual_filter() {
        // Use hash index on "active", but also filter on "age" in memory.
        let store = sample_store();
        let hash_idx = build_hash_index(&store, "active");
        let mut hash_map = HashMap::new();
        hash_map.insert("active".to_string(), hash_idx);

        let q = Query::collection("users").filter(Filter::and(vec![
            Filter::eq("active", Value::Bool(true)),
            Filter::gt("age", Value::Int(29)),
        ]));
        let indexes = vec![IndexInfo {
            field: "active".into(),
            kind: IndexKind::Hash,
            entry_count: 5,
        }];
        let plan = QueryPlanner::plan(&q, &indexes, store.count());
        assert!(matches!(plan.strategy, ScanStrategy::HashLookup { .. }));

        let btree_map = HashMap::new();
        let idx_set = IndexSet {
            hash: &hash_map,
            btree: &btree_map,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        // active=true AND age>29 → Alice(30), Charlie(35)
        assert_eq!(results.len(), 2);
        for doc in &results {
            if let Some(Value::Int(age)) = doc.get("age") {
                assert!(*age > 29);
            }
        }
    }

    // ── Streaming / bounded execution (2A.2) ──

    /// Wraps a store and counts how many documents were actually fetched.
    struct CountingStore {
        inner: MemDocStore,
        fetches: std::cell::Cell<usize>,
    }

    impl DocumentStore for CountingStore {
        fn get_document(&self, id: DocId) -> Option<Document> {
            self.fetches.set(self.fetches.get() + 1);
            self.inner.get_document(id)
        }

        fn all_doc_ids(&self) -> Vec<DocId> {
            self.inner.all_doc_ids()
        }
    }

    #[test]
    fn unsorted_limit_stops_fetching_early() {
        let mut inner = MemDocStore::new();
        for i in 1..=100u64 {
            inner.insert(make_user(i, &format!("U{i}"), i as i64, true));
        }
        let store = CountingStore {
            inner,
            fetches: std::cell::Cell::new(0),
        };

        let q = Query::collection("users").offset(1).limit(2);
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert_eq!(results.len(), 2);
        // offset 1 + limit 2 → exactly 3 documents fetched, not 100.
        assert_eq!(store.fetches.get(), 3);
    }

    #[test]
    fn topk_matches_full_sort_including_ties() {
        let mut store = MemDocStore::new();
        // Duplicate ages force tie-breaking: stable sort keeps id order.
        store.insert(make_user(1, "A", 30, true));
        store.insert(make_user(2, "B", 25, true));
        store.insert(make_user(3, "C", 30, true));
        store.insert(make_user(4, "D", 25, true));
        store.insert(make_user(5, "E", 40, true));

        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        for desc in [false, true] {
            let sort = if desc {
                Sort::desc("age")
            } else {
                Sort::asc("age")
            };
            let full = Query::collection("users").sort(sort.clone());
            let full_plan = QueryPlanner::plan(&full, &[], store.count());
            let expected: Vec<DocId> = QueryExecutor::execute(&full, &full_plan, &store, &idx_set)
                .into_iter()
                .map(|d| d.id)
                .take(3)
                .collect();

            let limited = Query::collection("users").sort(sort).limit(3);
            let plan = QueryPlanner::plan(&limited, &[], store.count());
            let got: Vec<DocId> = QueryExecutor::execute(&limited, &plan, &store, &idx_set)
                .into_iter()
                .map(|d| d.id)
                .collect();
            assert_eq!(got, expected, "top-k must match full sort (desc={desc})");
        }
    }

    #[test]
    fn sorted_limit_with_offset_beyond_results_is_empty() {
        let store = sample_store();
        let q = Query::collection("users")
            .sort(Sort::asc("age"))
            .offset(100)
            .limit(5);
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };
        assert!(QueryExecutor::execute(&q, &plan, &store, &idx_set).is_empty());
    }

    #[test]
    fn zero_limit_returns_empty() {
        let store = sample_store();
        for q in [
            Query::collection("users").limit(0),
            Query::collection("users").sort(Sort::asc("age")).limit(0),
        ] {
            let plan = QueryPlanner::plan(&q, &[], store.count());
            let (hash, btree) = empty_indexes();
            let idx_set = IndexSet {
                hash: &hash,
                btree: &btree,
            };
            assert!(QueryExecutor::execute(&q, &plan, &store, &idx_set).is_empty());
        }
    }

    #[test]
    fn empty_store() {
        let store = MemDocStore::new();
        let q =
            Query::collection("users").filter(Filter::eq("name", Value::String("Alice".into())));
        let plan = QueryPlanner::plan(&q, &[], store.count());
        let (hash, btree) = empty_indexes();
        let idx_set = IndexSet {
            hash: &hash,
            btree: &btree,
        };

        let results = QueryExecutor::execute(&q, &plan, &store, &idx_set);
        assert!(results.is_empty());
    }
}
