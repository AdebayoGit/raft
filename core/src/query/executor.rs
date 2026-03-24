//! Query executor — runs a [`QueryPlan`] against a [`DocumentStore`].
//!
//! Execution flow:
//! 1. Fetch candidate documents (via index or full scan).
//! 2. Apply remaining filter predicates in memory.
//! 3. Sort results.
//! 4. Apply offset and limit.

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

        // Step 2: Fetch documents and apply filter.
        let mut results: Vec<Document> = candidate_ids
            .into_iter()
            .filter_map(|id| store.get_document(id))
            .filter(|doc| match query.get_filter() {
                Some(filter) => filter.matches(&|field_name: &str| doc.get(field_name).cloned()),
                None => true,
            })
            .collect();

        // Step 3: Sort.
        if let Some(sort) = query.get_sort() {
            let field_name = sort.field.clone();
            let desc = sort.direction == SortDirection::Descending;
            results.sort_by(|a, b| {
                let va = a.get(&field_name);
                let vb = b.get(&field_name);
                let ord = match (va, vb) {
                    (Some(a_val), Some(b_val)) => a_val.partial_cmp(b_val).unwrap_or(std::cmp::Ordering::Equal),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if desc { ord.reverse() } else { ord }
            });
        }

        // Step 4: Offset.
        if let Some(offset) = query.get_offset() {
            if offset >= results.len() {
                return Vec::new();
            }
            results = results.into_iter().skip(offset).collect();
        }

        // Step 5: Limit.
        if let Some(limit) = query.get_limit() {
            results.truncate(limit);
        }

        results
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
    use crate::query::Filter;
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
        assert!(results.iter().all(|d| d.get("active") == Some(&Value::Bool(true))));
    }

    // ── Hash index ──

    #[test]
    fn hash_index_eq_lookup() {
        let store = sample_store();
        let hash_idx = build_hash_index(&store, "name");
        let mut hash_map = HashMap::new();
        hash_map.insert("name".to_string(), hash_idx);

        let q = Query::collection("users")
            .filter(Filter::eq("name", Value::String("Alice".into())));
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

        let q = Query::collection("users")
            .filter(Filter::gte("age", Value::Int(28)));
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
        let q = Query::collection("users")
            .sort(Sort::asc("age"))
            .limit(3);
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
        assert_eq!(
            results[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
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

    #[test]
    fn empty_store() {
        let store = MemDocStore::new();
        let q = Query::collection("users")
            .filter(Filter::eq("name", Value::String("Alice".into())));
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
