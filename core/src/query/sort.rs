//! Sort specification for query results.

use serde::{Deserialize, Serialize};

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Specifies how query results should be ordered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sort {
    pub field: String,
    pub direction: SortDirection,
}

impl Sort {
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Ascending,
        }
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Descending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asc_sort() {
        let s = Sort::asc("created_at");
        assert_eq!(s.field, "created_at");
        assert_eq!(s.direction, SortDirection::Ascending);
    }

    #[test]
    fn desc_sort() {
        let s = Sort::desc("score");
        assert_eq!(s.field, "score");
        assert_eq!(s.direction, SortDirection::Descending);
    }
}
