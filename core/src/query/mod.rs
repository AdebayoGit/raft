//! Typed predicate query engine — no SQL.
//!
//! Queries are built using a fluent [`Query`] API with optional
//! [`Filter`], [`Sort`], [`Limit`], and [`Offset`]. The [`QueryPlanner`]
//! inspects available indexes to pick the cheapest execution strategy, and
//! the [`QueryExecutor`] runs the plan against a [`DocumentStore`].
//!
//! ```text
//! Query::collection("users")
//!     .filter(Filter::eq("status", Value::String("active".into())))
//!     .sort(Sort::asc("created_at"))
//!     .limit(20)
//!     .offset(40)
//! ```

mod document;
mod executor;
mod filter;
mod planner;
mod sort;

pub use document::{Document, DocumentStore, Value};
pub use executor::{IndexSet, QueryExecutor};
pub use filter::{Filter, Predicate};
pub use planner::{QueryPlan, QueryPlanner, ScanStrategy};
pub use sort::{Sort, SortDirection};

/// A fully-specified query against a single collection.
#[derive(Debug, Clone)]
pub struct Query {
    collection: String,
    filter: Option<Filter>,
    sort: Option<Sort>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Query {
    /// Start building a query against `collection`.
    pub fn collection(name: impl Into<String>) -> Self {
        Self {
            collection: name.into(),
            filter: None,
            sort: None,
            limit: None,
            offset: None,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn sort(mut self, sort: Sort) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn collection_name(&self) -> &str {
        &self.collection
    }

    pub fn get_filter(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    pub fn get_sort(&self) -> Option<&Sort> {
        self.sort.as_ref()
    }

    pub fn get_limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn get_offset(&self) -> Option<usize> {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_minimal_query() {
        let q = Query::collection("users");
        assert_eq!(q.collection_name(), "users");
        assert!(q.get_filter().is_none());
        assert!(q.get_sort().is_none());
        assert!(q.get_limit().is_none());
        assert!(q.get_offset().is_none());
    }

    #[test]
    fn build_full_query() {
        let q = Query::collection("orders")
            .filter(Filter::eq("status", Value::String("shipped".into())))
            .sort(Sort::desc("created_at"))
            .limit(10)
            .offset(20);

        assert_eq!(q.collection_name(), "orders");
        assert!(q.get_filter().is_some());
        assert!(q.get_sort().is_some());
        assert_eq!(q.get_limit(), Some(10));
        assert_eq!(q.get_offset(), Some(20));
    }
}
