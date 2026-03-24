//! Query planner — selects the cheapest execution strategy.
//!
//! The planner inspects the query's filter predicates and the set of
//! available indexes to decide between:
//!
//! - **Full scan**: no usable index — read every document and filter in
//!   memory.
//! - **Hash lookup**: an equality predicate matches a hash index.
//! - **BTree range**: a range predicate matches a B-tree index.
//!
//! Cost is estimated as the number of documents that must be fetched from
//! the store. Index-assisted plans are always cheaper than a full scan.

use super::filter::Predicate;
use super::Query;

/// The strategy chosen by the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanStrategy {
    /// Read every document; filter in memory.
    FullScan,
    /// Use a hash index for exact-match lookup on `field`.
    HashLookup { field: String, key: Vec<u8> },
    /// Use a B-tree index for a range scan on `field`.
    BTreeRange {
        field: String,
        start: Option<Vec<u8>>,
        start_inclusive: bool,
        end: Option<Vec<u8>>,
        end_inclusive: bool,
    },
}

/// A query execution plan produced by [`QueryPlanner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub strategy: ScanStrategy,
    /// Estimated cost (lower is better). Full scan = total doc count.
    pub estimated_cost: usize,
}

/// Metadata the planner needs about an available index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    /// The field this index covers.
    pub field: String,
    /// The kind of index.
    pub kind: IndexKind,
    /// Approximate number of entries in the index (for cost estimation).
    pub entry_count: usize,
}

/// What kind of index is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Hash,
    BTree,
}

/// Stateless query planner.
pub struct QueryPlanner;

impl QueryPlanner {
    /// Produce a [`QueryPlan`] for the given query.
    ///
    /// `indexes` describes the indexes available for the query's collection.
    /// `total_docs` is the total number of documents in the collection
    /// (used as the full-scan cost).
    pub fn plan(query: &Query, indexes: &[IndexInfo], total_docs: usize) -> QueryPlan {
        let Some(filter) = query.get_filter() else {
            // No filter — must scan everything.
            return QueryPlan {
                strategy: ScanStrategy::FullScan,
                estimated_cost: total_docs,
            };
        };

        let conditions = filter.top_level_conditions();
        if conditions.is_empty() {
            return QueryPlan {
                strategy: ScanStrategy::FullScan,
                estimated_cost: total_docs,
            };
        }

        let mut best: Option<QueryPlan> = None;

        for cond in &conditions {
            for idx in indexes {
                if idx.field != cond.field {
                    continue;
                }

                let plan = match (idx.kind, cond.predicate) {
                    // Hash index + equality → direct lookup.
                    (IndexKind::Hash, Predicate::Eq) => {
                        let key = cond.value.to_index_bytes();
                        Some(QueryPlan {
                            strategy: ScanStrategy::HashLookup {
                                field: cond.field.clone(),
                                key,
                            },
                            // Assume very selective — cost 1.
                            estimated_cost: 1,
                        })
                    }
                    // BTree + equality → point lookup via range.
                    (IndexKind::BTree, Predicate::Eq) => {
                        let key = cond.value.to_index_bytes();
                        Some(QueryPlan {
                            strategy: ScanStrategy::BTreeRange {
                                field: cond.field.clone(),
                                start: Some(key.clone()),
                                start_inclusive: true,
                                end: Some(key),
                                end_inclusive: true,
                            },
                            estimated_cost: 1,
                        })
                    }
                    // BTree + range predicates.
                    (IndexKind::BTree, Predicate::Gt) => {
                        let key = cond.value.to_index_bytes();
                        Some(QueryPlan {
                            strategy: ScanStrategy::BTreeRange {
                                field: cond.field.clone(),
                                start: Some(key),
                                start_inclusive: false,
                                end: None,
                                end_inclusive: false,
                            },
                            estimated_cost: total_docs / 3,
                        })
                    }
                    (IndexKind::BTree, Predicate::Gte) => {
                        let key = cond.value.to_index_bytes();
                        Some(QueryPlan {
                            strategy: ScanStrategy::BTreeRange {
                                field: cond.field.clone(),
                                start: Some(key),
                                start_inclusive: true,
                                end: None,
                                end_inclusive: false,
                            },
                            estimated_cost: total_docs / 3,
                        })
                    }
                    (IndexKind::BTree, Predicate::Lt) => {
                        let key = cond.value.to_index_bytes();
                        Some(QueryPlan {
                            strategy: ScanStrategy::BTreeRange {
                                field: cond.field.clone(),
                                start: None,
                                start_inclusive: false,
                                end: Some(key),
                                end_inclusive: false,
                            },
                            estimated_cost: total_docs / 3,
                        })
                    }
                    (IndexKind::BTree, Predicate::Lte) => {
                        let key = cond.value.to_index_bytes();
                        Some(QueryPlan {
                            strategy: ScanStrategy::BTreeRange {
                                field: cond.field.clone(),
                                start: None,
                                start_inclusive: false,
                                end: Some(key),
                                end_inclusive: true,
                            },
                            estimated_cost: total_docs / 3,
                        })
                    }
                    // Contains can't use any index efficiently.
                    (_, Predicate::Contains) => None,
                    // Hash index can't serve range predicates.
                    (IndexKind::Hash, _) => None,
                };

                if let Some(p) = plan {
                    let dominated = best.as_ref().map_or(true, |b| p.estimated_cost < b.estimated_cost);
                    if dominated {
                        best = Some(p);
                    }
                }
            }
        }

        best.unwrap_or(QueryPlan {
            strategy: ScanStrategy::FullScan,
            estimated_cost: total_docs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Filter, Value};

    fn hash_index(field: &str) -> IndexInfo {
        IndexInfo {
            field: field.into(),
            kind: IndexKind::Hash,
            entry_count: 100,
        }
    }

    fn btree_index(field: &str) -> IndexInfo {
        IndexInfo {
            field: field.into(),
            kind: IndexKind::BTree,
            entry_count: 100,
        }
    }

    #[test]
    fn no_filter_full_scan() {
        let q = Query::collection("users");
        let plan = QueryPlanner::plan(&q, &[hash_index("name")], 1000);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
        assert_eq!(plan.estimated_cost, 1000);
    }

    #[test]
    fn eq_with_hash_index() {
        let q = Query::collection("users")
            .filter(Filter::eq("status", Value::String("active".into())));
        let plan = QueryPlanner::plan(&q, &[hash_index("status")], 1000);
        assert!(matches!(plan.strategy, ScanStrategy::HashLookup { ref field, .. } if field == "status"));
        assert_eq!(plan.estimated_cost, 1);
    }

    #[test]
    fn eq_with_btree_index() {
        let q = Query::collection("users")
            .filter(Filter::eq("status", Value::String("active".into())));
        let plan = QueryPlanner::plan(&q, &[btree_index("status")], 1000);
        assert!(matches!(plan.strategy, ScanStrategy::BTreeRange { ref field, .. } if field == "status"));
        assert_eq!(plan.estimated_cost, 1);
    }

    #[test]
    fn gt_with_btree_index() {
        let q = Query::collection("users")
            .filter(Filter::gt("age", Value::Int(18)));
        let plan = QueryPlanner::plan(&q, &[btree_index("age")], 900);
        assert!(matches!(
            plan.strategy,
            ScanStrategy::BTreeRange {
                start_inclusive: false,
                end: None,
                ..
            }
        ));
        assert_eq!(plan.estimated_cost, 300); // 900 / 3
    }

    #[test]
    fn lt_with_btree_index() {
        let q = Query::collection("users")
            .filter(Filter::lt("age", Value::Int(65)));
        let plan = QueryPlanner::plan(&q, &[btree_index("age")], 900);
        assert!(matches!(
            plan.strategy,
            ScanStrategy::BTreeRange {
                start: None,
                end_inclusive: false,
                ..
            }
        ));
    }

    #[test]
    fn lte_with_btree_index() {
        let q = Query::collection("users")
            .filter(Filter::lte("score", Value::Float(99.0)));
        let plan = QueryPlanner::plan(&q, &[btree_index("score")], 300);
        assert!(matches!(
            plan.strategy,
            ScanStrategy::BTreeRange {
                end_inclusive: true,
                ..
            }
        ));
    }

    #[test]
    fn gte_with_btree_index() {
        let q = Query::collection("users")
            .filter(Filter::gte("score", Value::Float(50.0)));
        let plan = QueryPlanner::plan(&q, &[btree_index("score")], 300);
        assert!(matches!(
            plan.strategy,
            ScanStrategy::BTreeRange {
                start_inclusive: true,
                end: None,
                ..
            }
        ));
    }

    #[test]
    fn hash_index_cannot_serve_range() {
        let q = Query::collection("users")
            .filter(Filter::gt("age", Value::Int(18)));
        let plan = QueryPlanner::plan(&q, &[hash_index("age")], 1000);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
    }

    #[test]
    fn contains_falls_back_to_full_scan() {
        let q = Query::collection("users")
            .filter(Filter::contains("bio", Value::String("rust".into())));
        let plan = QueryPlanner::plan(&q, &[hash_index("bio"), btree_index("bio")], 500);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
    }

    #[test]
    fn no_matching_index_full_scan() {
        let q = Query::collection("users")
            .filter(Filter::eq("email", Value::String("a@b.com".into())));
        // Only have index on "name", not "email".
        let plan = QueryPlanner::plan(&q, &[hash_index("name")], 500);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
    }

    #[test]
    fn prefers_cheapest_plan() {
        // AND of two conditions: eq on hash (cost=1) vs gt on btree (cost=333).
        let q = Query::collection("users").filter(Filter::and(vec![
            Filter::eq("status", Value::String("active".into())),
            Filter::gt("age", Value::Int(18)),
        ]));
        let indexes = vec![hash_index("status"), btree_index("age")];
        let plan = QueryPlanner::plan(&q, &indexes, 1000);
        // Hash eq is cheapest.
        assert!(matches!(plan.strategy, ScanStrategy::HashLookup { .. }));
        assert_eq!(plan.estimated_cost, 1);
    }

    #[test]
    fn or_filter_full_scan() {
        // OR at the top level → planner can't extract simple conditions.
        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("status", Value::String("active".into())),
            Filter::eq("status", Value::String("trial".into())),
        ]));
        let plan = QueryPlanner::plan(&q, &[hash_index("status")], 500);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
    }
}
