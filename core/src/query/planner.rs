//! Query planner — selects the cheapest execution strategy.
//!
//! The planner inspects the query's filter predicates and the set of
//! available indexes to decide between:
//!
//! - **Full scan**: no usable index — read every document and filter in
//!   memory.
//! - **Hash lookup**: an equality predicate matches a hash index.
//! - **Hash union**: a top-level `Or` of equality predicates on one field
//!   matches a hash index — union of point lookups.
//! - **BTree range**: a range predicate matches a B-tree index.
//!
//! Cost is estimated as the number of documents that must be fetched from
//! the store, derived from each index's [`IndexInfo::entry_count`] (Q6c)
//! rather than fixed fractions of the collection size — a sparse index
//! can never yield more candidates than it has entries.

use super::filter::Predicate;
use super::Query;

/// The strategy chosen by the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanStrategy {
    /// Read every document; filter in memory.
    FullScan,
    /// Use a hash index for exact-match lookup on `field`.
    HashLookup { field: String, key: Vec<u8> },
    /// Union several hash-index point lookups on `field` — chosen for a
    /// top-level `Or` of equality conditions on the same field.
    HashUnion { field: String, keys: Vec<Vec<u8>> },
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

/// Assumed fraction of an index's entries matched by one equality lookup:
/// 1 in `EQ_SELECTIVITY`. Equality on an indexed field is treated as
/// highly selective, but cost still scales with index size instead of
/// being a hardcoded constant.
const EQ_SELECTIVITY: usize = 100;

/// Assumed fraction of an index's entries matched by an open range
/// predicate: 1 in `RANGE_SELECTIVITY`.
const RANGE_SELECTIVITY: usize = 3;

/// Stateless query planner.
pub struct QueryPlanner;

impl QueryPlanner {
    /// Estimated documents fetched by one equality point lookup on an
    /// index holding `entry_count` entries. Never below one fetch.
    fn eq_cost(entry_count: usize) -> usize {
        (entry_count / EQ_SELECTIVITY).max(1)
    }

    /// Estimated documents fetched by a range scan on an index holding
    /// `entry_count` entries, capped at the collection size.
    fn range_cost(entry_count: usize, total_docs: usize) -> usize {
        (entry_count / RANGE_SELECTIVITY)
            .max(1)
            .min(total_docs.max(1))
    }

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
            // Top-level `Or`: a same-field OR of equalities can still be
            // served from a hash index as a union of point lookups (Q6c).
            if let Some((field, values)) = filter.same_field_or_eqs() {
                let hash_idx = indexes
                    .iter()
                    .find(|idx| idx.field == field && idx.kind == IndexKind::Hash);
                if let Some(idx) = hash_idx {
                    let keys: Vec<Vec<u8>> = values.iter().map(|v| v.to_index_bytes()).collect();
                    let cost = (Self::eq_cost(idx.entry_count) * keys.len()).min(total_docs.max(1));
                    return QueryPlan {
                        strategy: ScanStrategy::HashUnion {
                            field: field.to_string(),
                            keys,
                        },
                        estimated_cost: cost,
                    };
                }
            }
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
                            estimated_cost: Self::eq_cost(idx.entry_count),
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
                            estimated_cost: Self::eq_cost(idx.entry_count),
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
                            estimated_cost: Self::range_cost(idx.entry_count, total_docs),
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
                            estimated_cost: Self::range_cost(idx.entry_count, total_docs),
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
                            estimated_cost: Self::range_cost(idx.entry_count, total_docs),
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
                            estimated_cost: Self::range_cost(idx.entry_count, total_docs),
                        })
                    }
                    // Contains can't use any index efficiently.
                    (_, Predicate::Contains) => None,
                    // Hash index can't serve range predicates.
                    (IndexKind::Hash, _) => None,
                };

                if let Some(p) = plan {
                    let dominated = best
                        .as_ref()
                        .is_none_or(|b| p.estimated_cost < b.estimated_cost);
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
        let q =
            Query::collection("users").filter(Filter::eq("status", Value::String("active".into())));
        let plan = QueryPlanner::plan(&q, &[hash_index("status")], 1000);
        assert!(
            matches!(plan.strategy, ScanStrategy::HashLookup { ref field, .. } if field == "status")
        );
        assert_eq!(plan.estimated_cost, 1);
    }

    #[test]
    fn eq_with_btree_index() {
        let q =
            Query::collection("users").filter(Filter::eq("status", Value::String("active".into())));
        let plan = QueryPlanner::plan(&q, &[btree_index("status")], 1000);
        assert!(
            matches!(plan.strategy, ScanStrategy::BTreeRange { ref field, .. } if field == "status")
        );
        assert_eq!(plan.estimated_cost, 1);
    }

    #[test]
    fn gt_with_btree_index() {
        let q = Query::collection("users").filter(Filter::gt("age", Value::Int(18)));
        let plan = QueryPlanner::plan(&q, &[btree_index("age")], 900);
        assert!(matches!(
            plan.strategy,
            ScanStrategy::BTreeRange {
                start_inclusive: false,
                end: None,
                ..
            }
        ));
        // Cost derives from the index's entry count, not the collection
        // size: 100 entries / RANGE_SELECTIVITY.
        assert_eq!(plan.estimated_cost, 33);
    }

    #[test]
    fn lt_with_btree_index() {
        let q = Query::collection("users").filter(Filter::lt("age", Value::Int(65)));
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
        let q = Query::collection("users").filter(Filter::lte("score", Value::Float(99.0)));
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
        let q = Query::collection("users").filter(Filter::gte("score", Value::Float(50.0)));
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
        let q = Query::collection("users").filter(Filter::gt("age", Value::Int(18)));
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
        let q =
            Query::collection("users").filter(Filter::eq("email", Value::String("a@b.com".into())));
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
    fn or_same_field_eqs_uses_hash_union() {
        // Same-field OR of equalities + hash index → union of lookups,
        // not a full scan (Q6c).
        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("status", Value::String("active".into())),
            Filter::eq("status", Value::String("trial".into())),
        ]));
        let plan = QueryPlanner::plan(&q, &[hash_index("status")], 500);
        // Golden plan: exact strategy and cost.
        assert_eq!(
            plan,
            QueryPlan {
                strategy: ScanStrategy::HashUnion {
                    field: "status".into(),
                    keys: vec![
                        Value::String("active".into()).to_index_bytes(),
                        Value::String("trial".into()).to_index_bytes(),
                    ],
                },
                // eq_cost(100) * 2 keys.
                estimated_cost: 2,
            }
        );
    }

    #[test]
    fn or_union_cost_capped_at_total_docs() {
        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("status", Value::String("a".into())),
            Filter::eq("status", Value::String("b".into())),
            Filter::eq("status", Value::String("c".into())),
        ]));
        let idx = IndexInfo {
            field: "status".into(),
            kind: IndexKind::Hash,
            entry_count: 1000,
        };
        // eq_cost(1000) = 10, * 3 keys = 30, capped at 20 docs.
        let plan = QueryPlanner::plan(&q, &[idx], 20);
        assert_eq!(plan.estimated_cost, 20);
    }

    #[test]
    fn or_mixed_fields_full_scan() {
        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("status", Value::String("active".into())),
            Filter::eq("plan", Value::String("free".into())),
        ]));
        let plan = QueryPlanner::plan(&q, &[hash_index("status"), hash_index("plan")], 500);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
    }

    #[test]
    fn or_without_hash_index_full_scan() {
        // Only a B-tree index exists — union plan needs a hash index.
        let q = Query::collection("users").filter(Filter::or(vec![
            Filter::eq("status", Value::String("active".into())),
            Filter::eq("status", Value::String("trial".into())),
        ]));
        let plan = QueryPlanner::plan(&q, &[btree_index("status")], 500);
        assert_eq!(plan.strategy, ScanStrategy::FullScan);
    }

    // ── Golden plans: exact QueryPlan snapshots (Q6b/Q6c acceptance) ──

    #[test]
    fn golden_plan_hash_eq() {
        let q =
            Query::collection("users").filter(Filter::eq("status", Value::String("active".into())));
        let plan = QueryPlanner::plan(&q, &[hash_index("status")], 1000);
        assert_eq!(
            plan,
            QueryPlan {
                strategy: ScanStrategy::HashLookup {
                    field: "status".into(),
                    key: Value::String("active".into()).to_index_bytes(),
                },
                estimated_cost: 1,
            }
        );
    }

    #[test]
    fn golden_plan_btree_range() {
        let q = Query::collection("users").filter(Filter::gte("age", Value::Int(18)));
        let plan = QueryPlanner::plan(&q, &[btree_index("age")], 1000);
        assert_eq!(
            plan,
            QueryPlan {
                strategy: ScanStrategy::BTreeRange {
                    field: "age".into(),
                    start: Some(Value::Int(18).to_index_bytes()),
                    start_inclusive: true,
                    end: None,
                    end_inclusive: false,
                },
                estimated_cost: 33,
            }
        );
    }

    #[test]
    fn golden_plan_full_scan() {
        let q = Query::collection("users");
        let plan = QueryPlanner::plan(&q, &[], 42);
        assert_eq!(
            plan,
            QueryPlan {
                strategy: ScanStrategy::FullScan,
                estimated_cost: 42,
            }
        );
    }

    #[test]
    fn sparse_index_range_cheaper_than_dense_assumption() {
        // A sparse index (few entries) must not be costed as a fixed
        // fraction of the whole collection.
        let sparse = IndexInfo {
            field: "age".into(),
            kind: IndexKind::BTree,
            entry_count: 6,
        };
        let q = Query::collection("users").filter(Filter::gt("age", Value::Int(18)));
        let plan = QueryPlanner::plan(&q, &[sparse], 9000);
        assert_eq!(plan.estimated_cost, 2); // 6 / RANGE_SELECTIVITY
    }
}
