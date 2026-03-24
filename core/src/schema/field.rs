//! Field definitions, types, and CRDT hints.

use serde::{Deserialize, Serialize};

/// Logical data type for a schema field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    /// UTF-8 string.
    String,
    /// Signed 64-bit integer.
    Int,
    /// 64-bit IEEE 754 float.
    Float,
    /// Boolean.
    Bool,
    /// Arbitrary binary blob.
    Bytes,
    /// Reference to a document in another collection (stored as a document ID).
    Reference,
    /// Ordered collection of values (backed by OR-Set).
    Collection,
}

/// The CRDT strategy used to resolve conflicts for a field.
///
/// The hint must be compatible with the [`FieldType`]:
/// - Scalar types (`String`, `Int`, `Float`, `Bool`, `Bytes`, `Reference`)
///   default to [`CrdtHint::Lww`].
/// - `Collection` fields must use [`CrdtHint::OrSet`].
/// - `Int` fields may optionally use [`CrdtHint::Counter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CrdtHint {
    /// Last-write-wins register — scalar conflict resolution by HLC timestamp.
    Lww,
    /// Observed-remove set — add-wins semantics for collections.
    OrSet,
    /// Per-device delta counter — merge by taking max delta per device.
    Counter,
}

impl CrdtHint {
    /// Returns `true` if this hint is valid for the given field type.
    pub fn is_compatible_with(self, field_type: FieldType) -> bool {
        match (self, field_type) {
            // LWW works for all scalar types
            (CrdtHint::Lww, FieldType::String) => true,
            (CrdtHint::Lww, FieldType::Int) => true,
            (CrdtHint::Lww, FieldType::Float) => true,
            (CrdtHint::Lww, FieldType::Bool) => true,
            (CrdtHint::Lww, FieldType::Bytes) => true,
            (CrdtHint::Lww, FieldType::Reference) => true,
            // OrSet is for collections
            (CrdtHint::OrSet, FieldType::Collection) => true,
            // Counter only makes sense for Int
            (CrdtHint::Counter, FieldType::Int) => true,
            _ => false,
        }
    }
}

/// Default CRDT hint for a field type.
impl From<FieldType> for CrdtHint {
    fn from(ft: FieldType) -> Self {
        match ft {
            FieldType::Collection => CrdtHint::OrSet,
            _ => CrdtHint::Lww,
        }
    }
}

/// A single field definition within a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDef {
    name: String,
    field_type: FieldType,
    crdt_hint: CrdtHint,
    required: bool,
}

impl FieldDef {
    /// Creates a new field definition.
    ///
    /// Callers should prefer [`SchemaBuilder`](super::SchemaBuilder) which
    /// validates compatibility automatically.
    pub(crate) fn new(
        name: String,
        field_type: FieldType,
        crdt_hint: CrdtHint,
        required: bool,
    ) -> Self {
        Self {
            name,
            field_type,
            crdt_hint,
            required,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_type(&self) -> FieldType {
        self.field_type
    }

    pub fn crdt_hint(&self) -> CrdtHint {
        self.crdt_hint
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_compatible_with_scalars() {
        for ft in [
            FieldType::String,
            FieldType::Int,
            FieldType::Float,
            FieldType::Bool,
            FieldType::Bytes,
            FieldType::Reference,
        ] {
            assert!(
                CrdtHint::Lww.is_compatible_with(ft),
                "LWW should be compatible with {ft:?}"
            );
        }
    }

    #[test]
    fn lww_not_compatible_with_collection() {
        assert!(!CrdtHint::Lww.is_compatible_with(FieldType::Collection));
    }

    #[test]
    fn orset_only_compatible_with_collection() {
        assert!(CrdtHint::OrSet.is_compatible_with(FieldType::Collection));
        assert!(!CrdtHint::OrSet.is_compatible_with(FieldType::String));
        assert!(!CrdtHint::OrSet.is_compatible_with(FieldType::Int));
    }

    #[test]
    fn counter_only_compatible_with_int() {
        assert!(CrdtHint::Counter.is_compatible_with(FieldType::Int));
        assert!(!CrdtHint::Counter.is_compatible_with(FieldType::String));
        assert!(!CrdtHint::Counter.is_compatible_with(FieldType::Collection));
    }

    #[test]
    fn default_hint_for_scalars_is_lww() {
        assert_eq!(CrdtHint::from(FieldType::String), CrdtHint::Lww);
        assert_eq!(CrdtHint::from(FieldType::Int), CrdtHint::Lww);
        assert_eq!(CrdtHint::from(FieldType::Bool), CrdtHint::Lww);
    }

    #[test]
    fn default_hint_for_collection_is_orset() {
        assert_eq!(CrdtHint::from(FieldType::Collection), CrdtHint::OrSet);
    }

    #[test]
    fn field_def_accessors() {
        let f = FieldDef::new("score".into(), FieldType::Int, CrdtHint::Counter, false);
        assert_eq!(f.name(), "score");
        assert_eq!(f.field_type(), FieldType::Int);
        assert_eq!(f.crdt_hint(), CrdtHint::Counter);
        assert!(!f.required());
    }

    #[test]
    fn serde_round_trip_field_type() {
        for ft in [
            FieldType::String,
            FieldType::Int,
            FieldType::Float,
            FieldType::Bool,
            FieldType::Bytes,
            FieldType::Reference,
            FieldType::Collection,
        ] {
            let json = serde_json::to_string(&ft).expect("serialize");
            let decoded: FieldType = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(ft, decoded);
        }
    }

    #[test]
    fn serde_round_trip_field_def() {
        let f = FieldDef::new("name".into(), FieldType::String, CrdtHint::Lww, true);
        let json = serde_json::to_string(&f).expect("serialize");
        let decoded: FieldDef = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(f, decoded);
    }
}
