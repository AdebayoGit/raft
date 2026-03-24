//! Live query subscriptions — re-evaluate a query on each mutation and
//! emit diffs when results change.
//!
//! A [`LiveQuery`] wraps a [`Query`] and a broadcast receiver from the
//! [`EventBus`]. Each time a [`MutationEvent`] arrives for the query's
//! collection, the query is re-executed and the previous result set is
//! diffed against the new one. Only non-empty diffs are emitted.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::broadcast;

use crate::index::DocId;
use crate::query::{Document, Query};

use super::event::MutationEvent;

/// The diff between two consecutive query result sets.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryDiff {
    /// Documents present in the new results but absent from the old.
    pub added: Vec<Document>,
    /// Documents present in the old results but absent from the new.
    pub removed: Vec<Document>,
    /// Documents present in both but with changed field values.
    pub updated: Vec<Document>,
}

impl QueryDiff {
    /// Returns `true` if the diff contains no changes.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

/// Trait abstracting query re-evaluation so `LiveQuery` can be tested
/// without a full storage engine.
pub trait QueryRunner: Send + Sync {
    /// Execute the query and return the current result set.
    fn execute(&self, query: &Query) -> Vec<Document>;
}

/// A live query subscription that yields [`QueryDiff`] items.
///
/// Created via [`LiveQuery::new`]. Call [`next_diff`](Self::next_diff)
/// repeatedly to receive diffs as mutations flow through the bus.
pub struct LiveQuery<R: QueryRunner> {
    query: Query,
    runner: Arc<R>,
    receiver: broadcast::Receiver<MutationEvent>,
    /// Snapshot of the previous result set, keyed by DocId for efficient
    /// diffing. Initialised on first poll.
    previous: Option<HashMap<DocId, Document>>,
}

impl<R: QueryRunner> LiveQuery<R> {
    /// Create a new live query.
    ///
    /// `runner` is called to re-evaluate the query whenever a relevant
    /// mutation arrives. `bus` is the event bus to subscribe to.
    pub fn new(query: Query, runner: Arc<R>, bus: &super::EventBus) -> Self {
        let receiver = bus.subscribe();
        Self {
            query,
            runner,
            receiver,
            previous: None,
        }
    }

    /// Poll for the next non-empty diff.
    ///
    /// Blocks (async) until a mutation event arrives for this query's
    /// collection and the re-evaluated results differ from the previous
    /// snapshot. Returns `None` when the event bus is closed.
    pub async fn next_diff(&mut self) -> Option<QueryDiff> {
        // Bootstrap: capture initial snapshot.
        if self.previous.is_none() {
            let results = self.runner.execute(&self.query);
            self.previous = Some(index_by_id(results));
        }

        loop {
            let event = match self.receiver.recv().await {
                Ok(e) => e,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // We missed events — force a full re-evaluation.
                    let new_results = self.runner.execute(&self.query);
                    let new_map = index_by_id(new_results);
                    let diff = compute_diff(self.previous.as_ref().unwrap(), &new_map);
                    self.previous = Some(new_map);
                    if !diff.is_empty() {
                        return Some(diff);
                    }
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return None;
                }
            };

            // Skip events for other collections.
            if event.collection != self.query.collection_name() {
                continue;
            }

            // Re-evaluate.
            let new_results = self.runner.execute(&self.query);
            let new_map = index_by_id(new_results);
            let diff = compute_diff(self.previous.as_ref().unwrap(), &new_map);
            self.previous = Some(new_map);

            if !diff.is_empty() {
                return Some(diff);
            }
            // If the mutation didn't change the result set (e.g. it
            // affected a doc that doesn't match the filter), loop and
            // wait for the next event.
        }
    }
}

/// Index a result set by DocId for O(1) lookups during diff.
fn index_by_id(docs: Vec<Document>) -> HashMap<DocId, Document> {
    docs.into_iter().map(|d| (d.id, d)).collect()
}

/// Compute the diff between `old` and `new` result maps.
fn compute_diff(
    old: &HashMap<DocId, Document>,
    new: &HashMap<DocId, Document>,
) -> QueryDiff {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut updated = Vec::new();

    for (id, new_doc) in new {
        match old.get(id) {
            None => added.push(new_doc.clone()),
            Some(old_doc) if old_doc != new_doc => updated.push(new_doc.clone()),
            Some(_) => {}
        }
    }

    for (id, old_doc) in old {
        if !new.contains_key(id) {
            removed.push(old_doc.clone());
        }
    }

    // Sort for deterministic output.
    added.sort_by_key(|d| d.id);
    removed.sort_by_key(|d| d.id);
    updated.sort_by_key(|d| d.id);

    QueryDiff {
        added,
        removed,
        updated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::DocId;
    use crate::query::{Document, Filter, Query, Value};
    use crate::reactive::{EventBus, MutationEvent};
    use std::sync::{Arc, Mutex};

    /// A test query runner backed by a mutable document list.
    struct MockRunner {
        docs: Mutex<Vec<Document>>,
        query_count: Mutex<usize>,
    }

    impl MockRunner {
        fn new(docs: Vec<Document>) -> Self {
            Self {
                docs: Mutex::new(docs),
                query_count: Mutex::new(0),
            }
        }

        fn set_docs(&self, docs: Vec<Document>) {
            *self.docs.lock().unwrap() = docs;
        }

        fn query_count(&self) -> usize {
            *self.query_count.lock().unwrap()
        }
    }

    impl QueryRunner for MockRunner {
        fn execute(&self, query: &Query) -> Vec<Document> {
            *self.query_count.lock().unwrap() += 1;
            let docs = self.docs.lock().unwrap();
            docs.iter()
                .filter(|doc| match query.get_filter() {
                    Some(filter) => {
                        filter.matches(&|field_name: &str| doc.get(field_name).cloned())
                    }
                    None => true,
                })
                .cloned()
                .collect()
        }
    }

    fn user(id: u64, name: &str, active: bool) -> Document {
        Document::new(DocId(id))
            .with_field("name", Value::String(name.into()))
            .with_field("active", Value::Bool(active))
    }

    // ── compute_diff unit tests ──

    #[test]
    fn diff_identical_is_empty() {
        let old = index_by_id(vec![user(1, "Alice", true)]);
        let new = index_by_id(vec![user(1, "Alice", true)]);
        assert!(compute_diff(&old, &new).is_empty());
    }

    #[test]
    fn diff_added() {
        let old = index_by_id(vec![user(1, "Alice", true)]);
        let new = index_by_id(vec![user(1, "Alice", true), user(2, "Bob", true)]);
        let diff = compute_diff(&old, &new);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].id, DocId(2));
        assert!(diff.removed.is_empty());
        assert!(diff.updated.is_empty());
    }

    #[test]
    fn diff_removed() {
        let old = index_by_id(vec![user(1, "Alice", true), user(2, "Bob", true)]);
        let new = index_by_id(vec![user(1, "Alice", true)]);
        let diff = compute_diff(&old, &new);
        assert!(diff.added.is_empty());
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].id, DocId(2));
    }

    #[test]
    fn diff_updated() {
        let old = index_by_id(vec![user(1, "Alice", true)]);
        let new = index_by_id(vec![user(1, "Alice", false)]);
        let diff = compute_diff(&old, &new);
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.updated.len(), 1);
        assert_eq!(diff.updated[0].id, DocId(1));
    }

    #[test]
    fn diff_mixed_changes() {
        let old = index_by_id(vec![
            user(1, "Alice", true),
            user(2, "Bob", true),
            user(3, "Charlie", false),
        ]);
        let new = index_by_id(vec![
            user(1, "Alice", false),
            user(3, "Charlie", false),
            user(4, "Diana", true),
        ]);
        let diff = compute_diff(&old, &new);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].id, DocId(4));
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].id, DocId(2));
        assert_eq!(diff.updated.len(), 1);
        assert_eq!(diff.updated[0].id, DocId(1));
    }

    #[test]
    fn diff_empty_to_empty() {
        let old: HashMap<DocId, Document> = HashMap::new();
        let new: HashMap<DocId, Document> = HashMap::new();
        assert!(compute_diff(&old, &new).is_empty());
    }

    #[test]
    fn diff_empty_to_populated() {
        let old: HashMap<DocId, Document> = HashMap::new();
        let new = index_by_id(vec![user(1, "Alice", true)]);
        assert_eq!(compute_diff(&old, &new).added.len(), 1);
    }

    #[test]
    fn diff_populated_to_empty() {
        let old = index_by_id(vec![user(1, "Alice", true)]);
        let new: HashMap<DocId, Document> = HashMap::new();
        assert_eq!(compute_diff(&old, &new).removed.len(), 1);
    }

    // ── LiveQuery async tests ──
    //
    // These use tokio::spawn to publish events from a separate task,
    // ensuring the broadcast receiver's recv() can be polled concurrently.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_emits_diff_on_insert() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![user(1, "Alice", true)]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            runner2.set_docs(vec![user(1, "Alice", true), user(2, "Bob", true)]);
            bus2.publish(MutationEvent::insert("users", DocId(2)));
        });

        let diff = lq.next_diff().await.unwrap();
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].id, DocId(2));
        assert!(diff.removed.is_empty());
        assert!(diff.updated.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_emits_diff_on_delete() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![
            user(1, "Alice", true),
            user(2, "Bob", true),
        ]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            runner2.set_docs(vec![user(1, "Alice", true)]);
            bus2.publish(MutationEvent::delete("users", DocId(2)));
        });

        let diff = lq.next_diff().await.unwrap();
        assert!(diff.added.is_empty());
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].id, DocId(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_emits_diff_on_update() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![user(1, "Alice", true)]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            runner2.set_docs(vec![user(1, "Alice", false)]);
            bus2.publish(MutationEvent::update("users", DocId(1)));
        });

        let diff = lq.next_diff().await.unwrap();
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.updated.len(), 1);
        assert_eq!(diff.updated[0].get("active"), Some(&Value::Bool(false)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_skips_irrelevant_collections() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![user(1, "Alice", true)]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            // Irrelevant collection first.
            bus2.publish(MutationEvent::insert("orders", DocId(99)));
            // Then relevant.
            runner2.set_docs(vec![user(1, "Alice", true), user(2, "New", true)]);
            bus2.publish(MutationEvent::insert("users", DocId(2)));
        });

        let diff = lq.next_diff().await.unwrap();
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].id, DocId(2));
        // Bootstrap + one re-eval for the users event. Orders event skipped.
        assert_eq!(runner.query_count(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_suppresses_empty_diff() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![user(1, "Alice", true)]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            // Mutation that doesn't change results.
            bus2.publish(MutationEvent::update("users", DocId(1)));
            // Small yield to let the live query process the first event.
            tokio::task::yield_now().await;
            // Mutation that does change results.
            runner2.set_docs(vec![user(1, "Alice", true), user(2, "Bob", true)]);
            bus2.publish(MutationEvent::insert("users", DocId(2)));
        });

        let diff = lq.next_diff().await.unwrap();
        // Should have skipped the no-op diff.
        assert_eq!(diff.added.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_with_filter() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![
            user(1, "Alice", true),
            user(2, "Bob", false),
        ]));
        let query =
            Query::collection("users").filter(Filter::eq("active", Value::Bool(true)));
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            // Bob becomes active → now matches the filter.
            runner2.set_docs(vec![user(1, "Alice", true), user(2, "Bob", true)]);
            bus2.publish(MutationEvent::update("users", DocId(2)));
        });

        let diff = lq.next_diff().await.unwrap();
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].id, DocId(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_filter_removes_doc_from_results() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![
            user(1, "Alice", true),
            user(2, "Bob", true),
        ]));
        let query =
            Query::collection("users").filter(Filter::eq("active", Value::Bool(true)));
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            // Bob becomes inactive → leaves the filtered result set.
            runner2.set_docs(vec![user(1, "Alice", true), user(2, "Bob", false)]);
            bus2.publish(MutationEvent::update("users", DocId(2)));
        });

        let diff = lq.next_diff().await.unwrap();
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].id, DocId(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_multiple_diffs() {
        let bus = Arc::new(EventBus::new());
        let runner = Arc::new(MockRunner::new(vec![user(1, "Alice", true)]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        // First mutation.
        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            runner2.set_docs(vec![user(1, "Alice", true), user(2, "Bob", true)]);
            bus2.publish(MutationEvent::insert("users", DocId(2)));
        });

        let diff1 = lq.next_diff().await.unwrap();
        assert_eq!(diff1.added.len(), 1);

        // Second mutation.
        let bus3 = bus.clone();
        let runner3 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            runner3.set_docs(vec![
                user(1, "Alice", true),
                user(2, "Bob", true),
                user(3, "Charlie", true),
            ]);
            bus3.publish(MutationEvent::insert("users", DocId(3)));
        });

        let diff2 = lq.next_diff().await.unwrap();
        assert_eq!(diff2.added.len(), 1);
        assert_eq!(diff2.added[0].id, DocId(3));
    }

    #[tokio::test]
    async fn live_query_returns_none_when_bus_closed() {
        let bus = EventBus::new();
        let runner = Arc::new(MockRunner::new(vec![]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner, &bus);

        drop(bus);

        let result = lq.next_diff().await;
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_query_handles_lag() {
        let bus = Arc::new(EventBus::with_capacity(2));
        let runner = Arc::new(MockRunner::new(vec![user(1, "Alice", true)]));
        let query = Query::collection("users");
        let mut lq = LiveQuery::new(query, runner.clone(), &bus);

        let bus2 = bus.clone();
        let runner2 = runner.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            // Flood events to force lag.
            for i in 2..10u64 {
                runner2.set_docs(vec![user(1, "Alice", true), user(i, "X", true)]);
                bus2.publish(MutationEvent::insert("users", DocId(i)));
            }
        });

        let diff = lq.next_diff().await.unwrap();
        assert!(!diff.is_empty());
    }
}
