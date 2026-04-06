//! Filter predicates — typed, composable, no SQL strings.

use super::document::Value;

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Predicate {
    Eq,
    Lt,
    Gt,
    Lte,
    Gte,
    Contains,
}

/// A single filter condition on one field.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterCondition {
    pub field: String,
    pub predicate: Predicate,
    pub value: Value,
}

/// A composable filter that can be a single condition or a boolean
/// combination of conditions.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    Condition(FilterCondition),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

impl Filter {
    // ── Convenience constructors ──────────────────────────────────────

    pub fn eq(field: impl Into<String>, value: Value) -> Self {
        Self::condition(field, Predicate::Eq, value)
    }

    pub fn lt(field: impl Into<String>, value: Value) -> Self {
        Self::condition(field, Predicate::Lt, value)
    }

    pub fn gt(field: impl Into<String>, value: Value) -> Self {
        Self::condition(field, Predicate::Gt, value)
    }

    pub fn lte(field: impl Into<String>, value: Value) -> Self {
        Self::condition(field, Predicate::Lte, value)
    }

    pub fn gte(field: impl Into<String>, value: Value) -> Self {
        Self::condition(field, Predicate::Gte, value)
    }

    pub fn contains(field: impl Into<String>, value: Value) -> Self {
        Self::condition(field, Predicate::Contains, value)
    }

    pub fn and(filters: Vec<Filter>) -> Self {
        Self::And(filters)
    }

    pub fn or(filters: Vec<Filter>) -> Self {
        Self::Or(filters)
    }

    fn condition(field: impl Into<String>, predicate: Predicate, value: Value) -> Self {
        Self::Condition(FilterCondition {
            field: field.into(),
            predicate,
            value,
        })
    }

    // ── Evaluation ────────────────────────────────────────────────────

    /// Evaluate this filter against a document's field values.
    pub fn matches(&self, get_field: &dyn Fn(&str) -> Option<Value>) -> bool {
        match self {
            Filter::Condition(cond) => {
                let Some(doc_val) = get_field(&cond.field) else {
                    return false;
                };
                evaluate_predicate(&doc_val, cond.predicate, &cond.value)
            }
            Filter::And(filters) => filters.iter().all(|f| f.matches(get_field)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(get_field)),
        }
    }

    /// Extract the top-level conditions (non-recursive) for planner use.
    /// Returns conditions that are AND-combined at the top level.
    pub fn top_level_conditions(&self) -> Vec<&FilterCondition> {
        match self {
            Filter::Condition(c) => vec![c],
            Filter::And(filters) => filters
                .iter()
                .flat_map(|f| {
                    if let Filter::Condition(c) = f {
                        vec![c]
                    } else {
                        vec![]
                    }
                })
                .collect(),
            Filter::Or(_) => vec![],
        }
    }
}

fn evaluate_predicate(doc_val: &Value, predicate: Predicate, filter_val: &Value) -> bool {
    match predicate {
        Predicate::Eq => doc_val == filter_val,
        Predicate::Lt => doc_val
            .partial_cmp(filter_val)
            .is_some_and(|o| o.is_lt()),
        Predicate::Gt => doc_val
            .partial_cmp(filter_val)
            .is_some_and(|o| o.is_gt()),
        Predicate::Lte => doc_val
            .partial_cmp(filter_val)
            .is_some_and(|o| o.is_le()),
        Predicate::Gte => doc_val
            .partial_cmp(filter_val)
            .is_some_and(|o| o.is_ge()),
        Predicate::Contains => match (doc_val, filter_val) {
            (Value::String(haystack), Value::String(needle)) => haystack.contains(needle.as_str()),
            (Value::Bytes(haystack), Value::Bytes(needle)) => haystack
                .windows(needle.len())
                .any(|w| w == needle.as_slice()),
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field_getter(
        fields: &std::collections::HashMap<String, Value>,
    ) -> impl Fn(&str) -> Option<Value> + '_ {
        move |name: &str| fields.get(name).cloned()
    }

    fn sample_fields() -> std::collections::HashMap<String, Value> {
        let mut m = std::collections::HashMap::new();
        m.insert("name".into(), Value::String("Alice".into()));
        m.insert("age".into(), Value::Int(30));
        m.insert("score".into(), Value::Float(95.5));
        m.insert("active".into(), Value::Bool(true));
        m.insert("bio".into(), Value::String("Rust developer".into()));
        m
    }

    // ── Eq ──

    #[test]
    fn eq_string_matches() {
        let fields = sample_fields();
        let f = Filter::eq("name", Value::String("Alice".into()));
        assert!(f.matches(&field_getter(&fields)));
    }

    #[test]
    fn eq_string_no_match() {
        let fields = sample_fields();
        let f = Filter::eq("name", Value::String("Bob".into()));
        assert!(!f.matches(&field_getter(&fields)));
    }

    #[test]
    fn eq_int() {
        let fields = sample_fields();
        assert!(Filter::eq("age", Value::Int(30)).matches(&field_getter(&fields)));
        assert!(!Filter::eq("age", Value::Int(25)).matches(&field_getter(&fields)));
    }

    // ── Lt / Gt / Lte / Gte ──

    #[test]
    fn lt_int() {
        let fields = sample_fields();
        assert!(Filter::lt("age", Value::Int(40)).matches(&field_getter(&fields)));
        assert!(!Filter::lt("age", Value::Int(30)).matches(&field_getter(&fields)));
        assert!(!Filter::lt("age", Value::Int(20)).matches(&field_getter(&fields)));
    }

    #[test]
    fn gt_int() {
        let fields = sample_fields();
        assert!(Filter::gt("age", Value::Int(20)).matches(&field_getter(&fields)));
        assert!(!Filter::gt("age", Value::Int(30)).matches(&field_getter(&fields)));
    }

    #[test]
    fn lte_int() {
        let fields = sample_fields();
        assert!(Filter::lte("age", Value::Int(30)).matches(&field_getter(&fields)));
        assert!(Filter::lte("age", Value::Int(40)).matches(&field_getter(&fields)));
        assert!(!Filter::lte("age", Value::Int(20)).matches(&field_getter(&fields)));
    }

    #[test]
    fn gte_float() {
        let fields = sample_fields();
        assert!(Filter::gte("score", Value::Float(95.5)).matches(&field_getter(&fields)));
        assert!(Filter::gte("score", Value::Float(90.0)).matches(&field_getter(&fields)));
        assert!(!Filter::gte("score", Value::Float(96.0)).matches(&field_getter(&fields)));
    }

    // ── Contains ──

    #[test]
    fn contains_string() {
        let fields = sample_fields();
        assert!(Filter::contains("bio", Value::String("Rust".into())).matches(&field_getter(&fields)));
        assert!(!Filter::contains("bio", Value::String("Python".into())).matches(&field_getter(&fields)));
    }

    #[test]
    fn contains_cross_type_returns_false() {
        let fields = sample_fields();
        assert!(!Filter::contains("age", Value::String("30".into())).matches(&field_getter(&fields)));
    }

    // ── Missing field ──

    #[test]
    fn missing_field_never_matches() {
        let fields = sample_fields();
        assert!(!Filter::eq("nonexistent", Value::Int(0)).matches(&field_getter(&fields)));
    }

    // ── And / Or ──

    #[test]
    fn and_all_true() {
        let fields = sample_fields();
        let f = Filter::and(vec![
            Filter::eq("name", Value::String("Alice".into())),
            Filter::gte("age", Value::Int(18)),
        ]);
        assert!(f.matches(&field_getter(&fields)));
    }

    #[test]
    fn and_one_false() {
        let fields = sample_fields();
        let f = Filter::and(vec![
            Filter::eq("name", Value::String("Alice".into())),
            Filter::gt("age", Value::Int(50)),
        ]);
        assert!(!f.matches(&field_getter(&fields)));
    }

    #[test]
    fn or_one_true() {
        let fields = sample_fields();
        let f = Filter::or(vec![
            Filter::eq("name", Value::String("Bob".into())),
            Filter::eq("name", Value::String("Alice".into())),
        ]);
        assert!(f.matches(&field_getter(&fields)));
    }

    #[test]
    fn or_all_false() {
        let fields = sample_fields();
        let f = Filter::or(vec![
            Filter::eq("name", Value::String("Bob".into())),
            Filter::eq("name", Value::String("Charlie".into())),
        ]);
        assert!(!f.matches(&field_getter(&fields)));
    }

    // ── top_level_conditions ──

    #[test]
    fn top_level_single_condition() {
        let f = Filter::eq("x", Value::Int(1));
        let conds = f.top_level_conditions();
        assert_eq!(conds.len(), 1);
        assert_eq!(conds[0].field, "x");
    }

    #[test]
    fn top_level_and_conditions() {
        let f = Filter::and(vec![
            Filter::eq("a", Value::Int(1)),
            Filter::gt("b", Value::Int(2)),
        ]);
        let conds = f.top_level_conditions();
        assert_eq!(conds.len(), 2);
    }

    #[test]
    fn top_level_or_returns_empty() {
        let f = Filter::or(vec![
            Filter::eq("a", Value::Int(1)),
            Filter::eq("b", Value::Int(2)),
        ]);
        assert!(f.top_level_conditions().is_empty());
    }
}
