//! Schema system — typed field definitions with CRDT hints and versioning.
//!
//! A [`Schema`] is a named collection of [`FieldDef`] entries. Each field
//! carries a [`FieldType`] (the logical data type) and a [`CrdtHint`] (the
//! CRDT strategy used for conflict resolution). Schemas are immutable once
//! built; use [`SchemaBuilder`] to construct them.
//!
//! Schemas are versioned via [`SchemaVersion`]. Additive changes (new fields)
//! are always safe. Breaking changes (removing or retyping a field) require
//! bumping the version. The [`validate_evolution`] function enforces this.

mod builder;
mod error;
mod field;
mod version;

pub use builder::SchemaBuilder;
pub use error::SchemaError;
pub use field::{CrdtHint, FieldDef, FieldType};
pub use version::{validate_evolution, EvolutionResult, SchemaVersion};

use serde::{Deserialize, Serialize};

use crate::sync::SyncAuthority;

/// A named, versioned collection of field definitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    version: SchemaVersion,
    fields: Vec<FieldDef>,
    /// Controls how this collection resolves conflicts during sync.
    /// Defaults to [`SyncAuthority::LocalFirst`] for backward compatibility.
    #[serde(default)]
    sync_authority: SyncAuthority,
}

impl Schema {
    /// Returns a builder for constructing a new schema.
    pub fn builder(name: impl Into<String>) -> SchemaBuilder {
        SchemaBuilder::new(name)
    }

    /// Schema name (typically a collection/document type name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Current schema version.
    pub fn version(&self) -> SchemaVersion {
        self.version
    }

    /// The ordered list of field definitions.
    pub fn fields(&self) -> &[FieldDef] {
        &self.fields
    }

    /// Look up a field by name.
    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        self.fields.iter().find(|f| f.name() == name)
    }

    /// Returns the number of fields.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// The sync authority mode for this collection.
    pub fn sync_authority(&self) -> SyncAuthority {
        self.sync_authority
    }
}
