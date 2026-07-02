//! Document validation against a declared [`Schema`].
//!
//! Called on every write path (put, transactional commit) when the target
//! collection has a registered schema. Violations are typed — no panics.

use crate::query::{Document, Value};

use super::{FieldType, Schema};

/// A typed schema violation detected while validating a document write.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaViolation {
    #[error("unknown field `{field}` is not declared in the schema")]
    UnknownField { field: String },

    #[error("required field `{field}` is missing or null")]
    MissingRequiredField { field: String },

    #[error("field `{field}` expects {expected:?} but got {actual}")]
    TypeMismatch {
        field: String,
        expected: FieldType,
        actual: &'static str,
    },
}

/// Validate `doc` against `schema`.
///
/// Rules:
/// - Every document field must be declared in the schema.
/// - Non-null values must match the declared [`FieldType`]
///   (`Reference` fields hold document ids as [`Value::Int`];
///   `Collection` fields hold their serialised CRDT payload as
///   [`Value::Bytes`]).
/// - `Null` is allowed only on optional fields.
/// - Every `required` field must be present and non-null.
pub fn validate_document(schema: &Schema, doc: &Document) -> Result<(), SchemaViolation> {
    for (name, value) in &doc.fields {
        let Some(def) = schema.field(name) else {
            return Err(SchemaViolation::UnknownField {
                field: name.clone(),
            });
        };
        if matches!(value, Value::Null) {
            // Required-null is reported below as a missing field.
            continue;
        }
        if !value_matches_type(value, def.field_type()) {
            return Err(SchemaViolation::TypeMismatch {
                field: name.clone(),
                expected: def.field_type(),
                actual: value_type_name(value),
            });
        }
    }

    for def in schema.fields() {
        if def.required() && matches!(doc.get(def.name()), None | Some(Value::Null)) {
            return Err(SchemaViolation::MissingRequiredField {
                field: def.name().to_string(),
            });
        }
    }

    Ok(())
}

fn value_matches_type(value: &Value, field_type: FieldType) -> bool {
    matches!(
        (value, field_type),
        (Value::String(_), FieldType::String)
            | (Value::Int(_), FieldType::Int)
            | (Value::Float(_), FieldType::Float)
            | (Value::Bool(_), FieldType::Bool)
            | (Value::Bytes(_), FieldType::Bytes)
            | (Value::Int(_), FieldType::Reference)
            | (Value::Bytes(_), FieldType::Collection)
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "String",
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::Bool(_) => "Bool",
        Value::Bytes(_) => "Bytes",
        Value::Null => "Null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::DocId;

    fn schema() -> Schema {
        Schema::builder("users")
            .required_field("name", FieldType::String)
            .field("age", FieldType::Int)
            .field("score", FieldType::Float)
            .field("active", FieldType::Bool)
            .field("avatar", FieldType::Bytes)
            .field("team", FieldType::Reference)
            .build()
            .unwrap()
    }

    fn valid_doc() -> Document {
        Document::new(DocId(1))
            .with_field("name", Value::String("Alice".into()))
            .with_field("age", Value::Int(30))
    }

    #[test]
    fn accepts_valid_document() {
        assert_eq!(validate_document(&schema(), &valid_doc()), Ok(()));
    }

    #[test]
    fn rejects_unknown_field() {
        let doc = valid_doc().with_field("nickname", Value::String("Al".into()));
        assert_eq!(
            validate_document(&schema(), &doc),
            Err(SchemaViolation::UnknownField {
                field: "nickname".into()
            })
        );
    }

    #[test]
    fn rejects_wrong_type() {
        let doc = valid_doc().with_field("age", Value::String("thirty".into()));
        assert_eq!(
            validate_document(&schema(), &doc),
            Err(SchemaViolation::TypeMismatch {
                field: "age".into(),
                expected: FieldType::Int,
                actual: "String",
            })
        );
    }

    #[test]
    fn rejects_missing_required_field() {
        let doc = Document::new(DocId(1)).with_field("age", Value::Int(30));
        assert_eq!(
            validate_document(&schema(), &doc),
            Err(SchemaViolation::MissingRequiredField {
                field: "name".into()
            })
        );
    }

    #[test]
    fn rejects_null_on_required_field() {
        let doc = Document::new(DocId(1)).with_field("name", Value::Null);
        assert_eq!(
            validate_document(&schema(), &doc),
            Err(SchemaViolation::MissingRequiredField {
                field: "name".into()
            })
        );
    }

    #[test]
    fn allows_null_on_optional_field() {
        let doc = valid_doc().with_field("age", Value::Null);
        assert_eq!(validate_document(&schema(), &doc), Ok(()));
    }

    #[test]
    fn reference_field_holds_int_id() {
        let doc = valid_doc().with_field("team", Value::Int(7));
        assert_eq!(validate_document(&schema(), &doc), Ok(()));

        let bad = valid_doc().with_field("team", Value::String("seven".into()));
        assert!(matches!(
            validate_document(&schema(), &bad),
            Err(SchemaViolation::TypeMismatch { .. })
        ));
    }

    #[test]
    fn bytes_field_accepts_bytes() {
        let doc = valid_doc().with_field("avatar", Value::Bytes(vec![1, 2, 3]));
        assert_eq!(validate_document(&schema(), &doc), Ok(()));
    }
}
