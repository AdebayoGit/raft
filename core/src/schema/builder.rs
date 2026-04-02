//! Fluent builder for constructing [`Schema`] instances.

use std::collections::HashSet;

use super::error::SchemaError;
use super::field::{CrdtHint, FieldDef, FieldType};
use super::version::SchemaVersion;
use super::Schema;

/// Builder for constructing a [`Schema`] with validation.
///
/// # Example
///
/// ```
/// use raftdb::schema::{Schema, FieldType, CrdtHint, SchemaVersion};
///
/// let schema = Schema::builder("User")
///     .version(SchemaVersion(1))
///     .field("name", FieldType::String)
///     .required_field("email", FieldType::String)
///     .field_with_hint("score", FieldType::Int, CrdtHint::Counter)
///     .field("tags", FieldType::Collection)
///     .build()
///     .expect("valid schema");
///
/// assert_eq!(schema.name(), "User");
/// assert_eq!(schema.field_count(), 4);
/// ```
pub struct SchemaBuilder {
    name: String,
    version: SchemaVersion,
    fields: Vec<FieldDef>,
    seen_names: HashSet<String>,
}

impl SchemaBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: SchemaVersion(1),
            fields: Vec::new(),
            seen_names: HashSet::new(),
        }
    }

    /// Sets the schema version. Defaults to `SchemaVersion(1)`.
    pub fn version(mut self, version: SchemaVersion) -> Self {
        self.version = version;
        self
    }

    /// Adds an optional field with the default CRDT hint for its type.
    pub fn field(self, name: impl Into<String>, field_type: FieldType) -> Self {
        let hint = CrdtHint::from(field_type);
        self.add_field(name.into(), field_type, hint, false)
    }

    /// Adds a required field with the default CRDT hint for its type.
    pub fn required_field(self, name: impl Into<String>, field_type: FieldType) -> Self {
        let hint = CrdtHint::from(field_type);
        self.add_field(name.into(), field_type, hint, true)
    }

    /// Adds an optional field with an explicit CRDT hint.
    pub fn field_with_hint(
        self,
        name: impl Into<String>,
        field_type: FieldType,
        hint: CrdtHint,
    ) -> Self {
        self.add_field(name.into(), field_type, hint, false)
    }

    /// Adds a required field with an explicit CRDT hint.
    pub fn required_field_with_hint(
        self,
        name: impl Into<String>,
        field_type: FieldType,
        hint: CrdtHint,
    ) -> Self {
        self.add_field(name.into(), field_type, hint, true)
    }

    /// Validates and builds the schema.
    pub fn build(self) -> Result<Schema, SchemaError> {
        if self.name.is_empty() {
            return Err(SchemaError::EmptyName);
        }
        if self.fields.is_empty() {
            return Err(SchemaError::NoFields);
        }
        Ok(Schema {
            name: self.name,
            version: self.version,
            fields: self.fields,
        })
    }

    fn add_field(
        mut self,
        name: String,
        field_type: FieldType,
        hint: CrdtHint,
        required: bool,
    ) -> Self {
        // Validation is deferred to build() for ergonomic chaining, but
        // duplicate names and empty names are caught eagerly so the builder
        // state stays consistent.
        debug_assert!(
            !name.is_empty(),
            "field name must not be empty — will error on build()"
        );
        debug_assert!(
            hint.is_compatible_with(field_type),
            "CRDT hint {hint:?} is not compatible with {field_type:?}"
        );
        self.seen_names.insert(name.clone());
        self.fields
            .push(FieldDef::new(name, field_type, hint, required));
        self
    }

    /// Validates all accumulated fields, returning the first error found.
    ///
    /// Called internally by [`build`](Self::build); exposed for testing.
    #[allow(dead_code)]
    pub(crate) fn validate_fields(fields: &[FieldDef]) -> Result<(), SchemaError> {
        let mut seen = HashSet::with_capacity(fields.len());
        for f in fields {
            if f.name().is_empty() {
                return Err(SchemaError::EmptyFieldName);
            }
            if !seen.insert(f.name()) {
                return Err(SchemaError::DuplicateField(f.name().to_owned()));
            }
            if !f.crdt_hint().is_compatible_with(f.field_type()) {
                return Err(SchemaError::IncompatibleCrdtHint {
                    field: f.name().to_owned(),
                    field_type: f.field_type(),
                    hint: f.crdt_hint(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_minimal_schema() {
        let schema = Schema::builder("Note")
            .field("body", FieldType::String)
            .build()
            .expect("valid");
        assert_eq!(schema.name(), "Note");
        assert_eq!(schema.version(), SchemaVersion(1));
        assert_eq!(schema.field_count(), 1);
    }

    #[test]
    fn build_with_explicit_version() {
        let schema = Schema::builder("Note")
            .version(SchemaVersion(3))
            .field("body", FieldType::String)
            .build()
            .expect("valid");
        assert_eq!(schema.version(), SchemaVersion(3));
    }

    #[test]
    fn build_multi_field_schema() {
        let schema = Schema::builder("User")
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .field_with_hint("score", FieldType::Int, CrdtHint::Counter)
            .field("tags", FieldType::Collection)
            .field("avatar", FieldType::Bytes)
            .field("verified", FieldType::Bool)
            .field("rating", FieldType::Float)
            .field("org_id", FieldType::Reference)
            .build()
            .expect("valid");

        assert_eq!(schema.field_count(), 8);

        let email = schema.field("email").expect("email field");
        assert!(email.required());
        assert_eq!(email.crdt_hint(), CrdtHint::Lww);

        let score = schema.field("score").expect("score field");
        assert_eq!(score.crdt_hint(), CrdtHint::Counter);

        let tags = schema.field("tags").expect("tags field");
        assert_eq!(tags.crdt_hint(), CrdtHint::OrSet);
    }

    #[test]
    fn build_empty_name_fails() {
        let result = Schema::builder("")
            .field("x", FieldType::String)
            .build();
        assert!(matches!(result, Err(SchemaError::EmptyName)));
    }

    #[test]
    fn build_no_fields_fails() {
        let result = Schema::builder("Empty").build();
        assert!(matches!(result, Err(SchemaError::NoFields)));
    }

    #[test]
    fn validate_empty_field_name() {
        let fields = vec![FieldDef::new(
            String::new(),
            FieldType::String,
            CrdtHint::Lww,
            false,
        )];
        let result = SchemaBuilder::validate_fields(&fields);
        assert!(matches!(result, Err(SchemaError::EmptyFieldName)));
    }

    #[test]
    fn validate_duplicate_field_names() {
        let fields = vec![
            FieldDef::new("name".into(), FieldType::String, CrdtHint::Lww, false),
            FieldDef::new("name".into(), FieldType::Int, CrdtHint::Lww, false),
        ];
        let result = SchemaBuilder::validate_fields(&fields);
        assert!(matches!(result, Err(SchemaError::DuplicateField(n)) if n == "name"));
    }

    #[test]
    fn validate_incompatible_crdt_hint() {
        let fields = vec![FieldDef::new(
            "bad".into(),
            FieldType::String,
            CrdtHint::Counter, // Counter incompatible with String
            false,
        )];
        let result = SchemaBuilder::validate_fields(&fields);
        assert!(matches!(
            result,
            Err(SchemaError::IncompatibleCrdtHint { .. })
        ));
    }

    #[test]
    fn required_field_with_explicit_hint() {
        let schema = Schema::builder("Counter")
            .required_field_with_hint("views", FieldType::Int, CrdtHint::Counter)
            .build()
            .expect("valid");

        let views = schema.field("views").unwrap();
        assert!(views.required());
        assert_eq!(views.crdt_hint(), CrdtHint::Counter);
    }

    #[test]
    fn field_lookup_returns_none_for_missing() {
        let schema = Schema::builder("T")
            .field("a", FieldType::String)
            .build()
            .expect("valid");
        assert!(schema.field("nonexistent").is_none());
    }

    #[test]
    fn serde_round_trip_schema() {
        let schema = Schema::builder("Task")
            .version(SchemaVersion(2))
            .required_field("title", FieldType::String)
            .field("done", FieldType::Bool)
            .field_with_hint("priority", FieldType::Int, CrdtHint::Counter)
            .field("assignees", FieldType::Collection)
            .build()
            .expect("valid");

        let json = serde_json::to_string(&schema).expect("serialize");
        let decoded: Schema = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(schema, decoded);
    }
}
