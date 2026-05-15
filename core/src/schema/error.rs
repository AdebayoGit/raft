//! Schema error types.

/// Errors that can occur when building or evolving schemas.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("schema name must not be empty")]
    EmptyName,

    #[error("field name must not be empty")]
    EmptyFieldName,

    #[error("duplicate field name: `{0}`")]
    DuplicateField(String),

    #[error(
        "CRDT hint {hint:?} is not compatible with field type {field_type:?} on field `{field}`"
    )]
    IncompatibleCrdtHint {
        field: String,
        field_type: super::FieldType,
        hint: super::CrdtHint,
    },

    #[error("conflict strategy {strategy:?} is not compatible with field type {field_type:?} on field `{field}`")]
    IncompatibleConflictStrategy {
        field: String,
        field_type: super::FieldType,
        strategy: super::ConflictStrategy,
    },

    #[error("schema must contain at least one field")]
    NoFields,
}
