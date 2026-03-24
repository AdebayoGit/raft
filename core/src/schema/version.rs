//! Schema versioning and evolution validation.
//!
//! Additive changes (new fields) are always allowed without bumping the
//! version. Breaking changes (removed fields, changed field type, changed
//! CRDT hint) require a version bump.

use serde::{Deserialize, Serialize};

use super::Schema;

/// A monotonically increasing schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchemaVersion(pub u32);

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// A single incompatibility found when comparing two schema versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakingChange {
    /// A field present in the old schema was removed.
    FieldRemoved { field: String },
    /// A field's type changed between versions.
    FieldTypeChanged {
        field: String,
        old: super::FieldType,
        new: super::FieldType,
    },
    /// A field's CRDT hint changed between versions.
    CrdtHintChanged {
        field: String,
        old: super::CrdtHint,
        new: super::CrdtHint,
    },
    /// A field changed from optional to required (existing documents may
    /// lack the field).
    BecameRequired { field: String },
}

impl std::fmt::Display for BreakingChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BreakingChange::FieldRemoved { field } => {
                write!(f, "field `{field}` was removed")
            }
            BreakingChange::FieldTypeChanged { field, old, new } => {
                write!(f, "field `{field}` type changed from {old:?} to {new:?}")
            }
            BreakingChange::CrdtHintChanged { field, old, new } => {
                write!(
                    f,
                    "field `{field}` CRDT hint changed from {old:?} to {new:?}"
                )
            }
            BreakingChange::BecameRequired { field } => {
                write!(f, "field `{field}` changed from optional to required")
            }
        }
    }
}

/// The result of comparing an old schema to a new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionResult {
    /// No changes detected.
    Identical,
    /// Only additive changes (new optional fields). Safe without a version bump.
    Additive { new_fields: Vec<String> },
    /// Breaking changes detected. The new schema **must** have a higher version.
    Breaking {
        changes: Vec<BreakingChange>,
        new_fields: Vec<String>,
    },
}

/// Validates whether `new` is a valid evolution of `old`.
///
/// Returns [`EvolutionResult`] describing the kind of change. Callers should
/// check that if the result is `Breaking`, then `new.version() > old.version()`.
pub fn validate_evolution(old: &Schema, new: &Schema) -> EvolutionResult {
    let mut breaking = Vec::new();
    let mut new_fields = Vec::new();

    // Check every field in the old schema still exists with the same shape.
    for old_field in old.fields() {
        match new.field(old_field.name()) {
            None => {
                breaking.push(BreakingChange::FieldRemoved {
                    field: old_field.name().to_owned(),
                });
            }
            Some(new_field) => {
                if old_field.field_type() != new_field.field_type() {
                    breaking.push(BreakingChange::FieldTypeChanged {
                        field: old_field.name().to_owned(),
                        old: old_field.field_type(),
                        new: new_field.field_type(),
                    });
                }
                if old_field.crdt_hint() != new_field.crdt_hint() {
                    breaking.push(BreakingChange::CrdtHintChanged {
                        field: old_field.name().to_owned(),
                        old: old_field.crdt_hint(),
                        new: new_field.crdt_hint(),
                    });
                }
                if !old_field.required() && new_field.required() {
                    breaking.push(BreakingChange::BecameRequired {
                        field: old_field.name().to_owned(),
                    });
                }
            }
        }
    }

    // Detect new fields.
    for new_field in new.fields() {
        if old.field(new_field.name()).is_none() {
            new_fields.push(new_field.name().to_owned());
        }
    }

    if breaking.is_empty() && new_fields.is_empty() {
        EvolutionResult::Identical
    } else if breaking.is_empty() {
        EvolutionResult::Additive { new_fields }
    } else {
        EvolutionResult::Breaking {
            changes: breaking,
            new_fields,
        }
    }
}

/// Returns `true` if `new` is a valid successor to `old`:
/// - Additive changes are always valid.
/// - Breaking changes are valid only if the version was bumped.
pub fn is_valid_evolution(old: &Schema, new: &Schema) -> bool {
    match validate_evolution(old, new) {
        EvolutionResult::Identical | EvolutionResult::Additive { .. } => true,
        EvolutionResult::Breaking { .. } => new.version() > old.version(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{CrdtHint, FieldType, Schema};

    fn v1_user() -> Schema {
        Schema::builder("User")
            .version(SchemaVersion(1))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .field("age", FieldType::Int)
            .build()
            .expect("valid")
    }

    #[test]
    fn identical_schemas() {
        let a = v1_user();
        let b = v1_user();
        assert_eq!(validate_evolution(&a, &b), EvolutionResult::Identical);
    }

    #[test]
    fn additive_new_field() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(1))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .field("age", FieldType::Int)
            .field("bio", FieldType::String)
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Additive { new_fields } => {
                assert_eq!(new_fields, vec!["bio"]);
            }
            other => panic!("expected Additive, got {other:?}"),
        }
    }

    #[test]
    fn additive_multiple_new_fields() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(1))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .field("age", FieldType::Int)
            .field("bio", FieldType::String)
            .field("tags", FieldType::Collection)
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Additive { new_fields } => {
                assert_eq!(new_fields, vec!["bio", "tags"]);
            }
            other => panic!("expected Additive, got {other:?}"),
        }
    }

    #[test]
    fn breaking_field_removed() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(1))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            // "age" removed
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Breaking { changes, .. } => {
                assert!(changes
                    .iter()
                    .any(|c| matches!(c, BreakingChange::FieldRemoved { field } if field == "age")));
            }
            other => panic!("expected Breaking, got {other:?}"),
        }
    }

    #[test]
    fn breaking_field_type_changed() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(2))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .field("age", FieldType::String) // was Int
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Breaking { changes, .. } => {
                assert!(changes.iter().any(|c| matches!(
                    c,
                    BreakingChange::FieldTypeChanged {
                        field,
                        old: FieldType::Int,
                        new: FieldType::String,
                    } if field == "age"
                )));
            }
            other => panic!("expected Breaking, got {other:?}"),
        }
    }

    #[test]
    fn breaking_crdt_hint_changed() {
        let old = Schema::builder("Counter")
            .version(SchemaVersion(1))
            .field_with_hint("views", FieldType::Int, CrdtHint::Counter)
            .build()
            .expect("valid");

        let new = Schema::builder("Counter")
            .version(SchemaVersion(2))
            .field("views", FieldType::Int) // default LWW hint, was Counter
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Breaking { changes, .. } => {
                assert!(changes.iter().any(|c| matches!(
                    c,
                    BreakingChange::CrdtHintChanged {
                        field,
                        old: CrdtHint::Counter,
                        new: CrdtHint::Lww,
                    } if field == "views"
                )));
            }
            other => panic!("expected Breaking, got {other:?}"),
        }
    }

    #[test]
    fn breaking_optional_to_required() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(2))
            .required_field("email", FieldType::String)
            .required_field("name", FieldType::String) // was optional
            .field("age", FieldType::Int)
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Breaking { changes, .. } => {
                assert!(changes.iter().any(
                    |c| matches!(c, BreakingChange::BecameRequired { field } if field == "name")
                ));
            }
            other => panic!("expected Breaking, got {other:?}"),
        }
    }

    #[test]
    fn required_to_optional_is_not_breaking() {
        let old = v1_user(); // email is required
        let new = Schema::builder("User")
            .version(SchemaVersion(1))
            .field("email", FieldType::String) // now optional
            .field("name", FieldType::String)
            .field("age", FieldType::Int)
            .build()
            .expect("valid");

        // Relaxing a constraint is additive-safe (no breaking).
        assert_eq!(validate_evolution(&old, &new), EvolutionResult::Identical);
    }

    #[test]
    fn breaking_with_new_fields() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(2))
            .required_field("email", FieldType::String)
            // "name" removed — breaking
            .field("age", FieldType::Int)
            .field("avatar", FieldType::Bytes) // new field
            .build()
            .expect("valid");

        match validate_evolution(&old, &new) {
            EvolutionResult::Breaking {
                changes,
                new_fields,
            } => {
                assert!(changes.iter().any(
                    |c| matches!(c, BreakingChange::FieldRemoved { field } if field == "name")
                ));
                assert_eq!(new_fields, vec!["avatar"]);
            }
            other => panic!("expected Breaking, got {other:?}"),
        }
    }

    #[test]
    fn is_valid_evolution_additive_same_version() {
        let old = v1_user();
        let new = Schema::builder("User")
            .version(SchemaVersion(1))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .field("age", FieldType::Int)
            .field("bio", FieldType::String)
            .build()
            .expect("valid");

        assert!(is_valid_evolution(&old, &new));
    }

    #[test]
    fn is_valid_evolution_breaking_requires_version_bump() {
        let old = v1_user();

        // Same version — invalid
        let bad = Schema::builder("User")
            .version(SchemaVersion(1))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            // "age" removed
            .build()
            .expect("valid");
        assert!(!is_valid_evolution(&old, &bad));

        // Bumped version — valid
        let good = Schema::builder("User")
            .version(SchemaVersion(2))
            .required_field("email", FieldType::String)
            .field("name", FieldType::String)
            .build()
            .expect("valid");
        assert!(is_valid_evolution(&old, &good));
    }

    #[test]
    fn schema_version_display() {
        assert_eq!(SchemaVersion(1).to_string(), "v1");
        assert_eq!(SchemaVersion(42).to_string(), "v42");
    }

    #[test]
    fn schema_version_ordering() {
        assert!(SchemaVersion(1) < SchemaVersion(2));
        assert!(SchemaVersion(5) > SchemaVersion(3));
        assert_eq!(SchemaVersion(1), SchemaVersion(1));
    }

    #[test]
    fn breaking_change_display_messages() {
        let removed = BreakingChange::FieldRemoved {
            field: "age".into(),
        };
        assert_eq!(removed.to_string(), "field `age` was removed");

        let retyped = BreakingChange::FieldTypeChanged {
            field: "score".into(),
            old: FieldType::Int,
            new: FieldType::String,
        };
        assert!(retyped.to_string().contains("score"));

        let rehinted = BreakingChange::CrdtHintChanged {
            field: "views".into(),
            old: CrdtHint::Counter,
            new: CrdtHint::Lww,
        };
        assert!(rehinted.to_string().contains("views"));

        let required = BreakingChange::BecameRequired {
            field: "name".into(),
        };
        assert!(required.to_string().contains("name"));
    }
}
