//! Per-field conflict resolution strategies.
//!
//! In v0.1.0, conflict resolution was driven entirely by the per-collection
//! [`SyncAuthority`](crate::sync::SyncAuthority) plus the per-field
//! [`CrdtHint`](super::CrdtHint). v0.1.1 introduces a richer per-field
//! [`ConflictStrategy`] that lets developers pick the right tradeoff for
//! each field — full CRDT semantics, simple last-write-wins, server
//! authority, or a custom merge function — without sacrificing backward
//! compatibility.
//!
//! Existing schemas that do not opt in continue to behave exactly as
//! before: each field's strategy is derived from its [`FieldType`] and
//! [`CrdtHint`] via [`ConflictStrategy::default_for`].

use serde::{Deserialize, Serialize};

use super::field::{CrdtHint, FieldType};

/// How a field should resolve conflicts when two devices write
/// concurrently to the same document.
///
/// Strategies are declared per-field on the schema. The default for any
/// field — used when the schema builder is not given an explicit
/// strategy — is derived from the field's [`FieldType`] via
/// [`ConflictStrategy::default_for`], which preserves the v0.1.0
/// behaviour.
///
/// # Tradeoffs
///
/// | Strategy        | Storage cost | Data safety            | When to use                               |
/// |-----------------|--------------|------------------------|-------------------------------------------|
/// | `Crdt`          | High         | No data loss           | Counters, sets, true concurrent semantics |
/// | `LastWriteWins` | Low          | Loses concurrent write | Scalar fields where one-of-two is ok      |
/// | `ServerAuthority` | Low        | Discards local write   | Server-managed values (balances, prices)  |
/// | `Custom`        | Medium       | Developer-controlled   | Domain-specific resolution                |
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// CRDT-backed merge — mathematically convergent, no data loss,
    /// automatic. Higher storage overhead because per-device metadata is
    /// retained. Use for counters, sets, and registers that require true
    /// concurrent semantics.
    Crdt(CrdtKind),

    /// Last write wins by HLC timestamp. Simple, low overhead, silently
    /// loses concurrent writes. Use for scalar fields where losing one of
    /// two concurrent writes is acceptable.
    ///
    /// Ties on HLC are broken deterministically by `device_id` so all
    /// replicas converge to the same value.
    LastWriteWins,

    /// Server is authoritative. Client writes are tentative until
    /// acknowledged. On conflict the server value is enforced and the
    /// conflicting local write is discarded. Use for inventory counts,
    /// account balances, and other server-controlled state.
    ServerAuthority,

    /// Developer-provided merge function. On concurrent writes the
    /// resolver hook identified by [`CustomResolverId`] is invoked to
    /// produce the merged value. If no resolver is registered for the id
    /// the field is surfaced as conflicted on the document so the
    /// application can resolve it manually.
    Custom(CustomResolverId),
}

impl ConflictStrategy {
    /// Default strategy for a field of the given [`FieldType`].
    ///
    /// This preserves v0.1.0 behaviour — schemas that do not opt in to
    /// the per-field API see the same conflict resolution as before:
    ///
    /// | Field type    | Default strategy                    |
    /// |---------------|-------------------------------------|
    /// | `String`      | `Crdt(LwwRegister)`                 |
    /// | `Int`         | `Crdt(LwwRegister)`                 |
    /// | `Float`       | `Crdt(LwwRegister)`                 |
    /// | `Bool`        | `Crdt(LwwRegister)`                 |
    /// | `Bytes`       | `Crdt(LwwRegister)`                 |
    /// | `Reference`   | `Crdt(LwwRegister)`                 |
    /// | `Collection`  | `Crdt(OrSet)`                       |
    pub fn default_for(field_type: FieldType) -> Self {
        ConflictStrategy::Crdt(CrdtKind::from(field_type))
    }

    /// Strategy implied by an explicit [`CrdtHint`] — used by the schema
    /// builder when callers go through the legacy `field_with_hint` API.
    pub fn from_crdt_hint(hint: CrdtHint) -> Self {
        ConflictStrategy::Crdt(CrdtKind::from(hint))
    }

    /// Returns `true` if this strategy is valid for the given
    /// [`FieldType`]. Validation rules:
    ///
    /// - `Crdt(_)` delegates to [`CrdtHint::is_compatible_with`].
    /// - `LastWriteWins` is valid for any scalar field type.
    /// - `ServerAuthority` is valid for any field type.
    /// - `Custom` is valid for any field type — the resolver is the
    ///   developer's responsibility.
    pub fn is_compatible_with(&self, field_type: FieldType) -> bool {
        match self {
            ConflictStrategy::Crdt(kind) => CrdtHint::from(*kind).is_compatible_with(field_type),
            ConflictStrategy::LastWriteWins => !matches!(field_type, FieldType::Collection),
            ConflictStrategy::ServerAuthority => true,
            ConflictStrategy::Custom(_) => true,
        }
    }
}

/// Concrete CRDT primitive backing a [`ConflictStrategy::Crdt`] field.
///
/// Mirrors [`CrdtHint`] but is namespaced under [`ConflictStrategy`] so
/// the per-field API reads naturally. The two are interconvertible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CrdtKind {
    /// Last-write-wins register with full CRDT metadata (HLC + device id).
    /// Distinct from [`ConflictStrategy::LastWriteWins`], which is a
    /// metadata-free shortcut for scalar fields.
    LwwRegister,
    /// Observed-remove set — add-wins semantics for collections.
    OrSet,
    /// PN-counter — per-device delta counter with deterministic merge.
    Counter,
    // Future: causal tree for ordered text / rich content.
}

impl From<CrdtKind> for CrdtHint {
    fn from(kind: CrdtKind) -> Self {
        match kind {
            CrdtKind::LwwRegister => CrdtHint::Lww,
            CrdtKind::OrSet => CrdtHint::OrSet,
            CrdtKind::Counter => CrdtHint::Counter,
        }
    }
}

impl From<CrdtHint> for CrdtKind {
    fn from(hint: CrdtHint) -> Self {
        match hint {
            CrdtHint::Lww => CrdtKind::LwwRegister,
            CrdtHint::OrSet => CrdtKind::OrSet,
            CrdtHint::Counter => CrdtKind::Counter,
        }
    }
}

impl From<FieldType> for CrdtKind {
    fn from(ft: FieldType) -> Self {
        CrdtHint::from(ft).into()
    }
}

/// Stable identifier referencing a registered custom resolver.
///
/// The id is part of the schema and persists across runs — the resolver
/// itself is registered at runtime against the same id. Schemas can be
/// loaded before resolvers are registered; missing resolvers surface as
/// `Conflicted` outcomes rather than panics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CustomResolverId(pub String);

impl CustomResolverId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CustomResolverId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for CustomResolverId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for CustomResolverId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_for_scalar_is_lww_register() {
        for ft in [
            FieldType::String,
            FieldType::Int,
            FieldType::Float,
            FieldType::Bool,
            FieldType::Bytes,
            FieldType::Reference,
        ] {
            assert_eq!(
                ConflictStrategy::default_for(ft),
                ConflictStrategy::Crdt(CrdtKind::LwwRegister),
                "wrong default for {ft:?}"
            );
        }
    }

    #[test]
    fn default_for_collection_is_orset() {
        assert_eq!(
            ConflictStrategy::default_for(FieldType::Collection),
            ConflictStrategy::Crdt(CrdtKind::OrSet)
        );
    }

    #[test]
    fn lww_strategy_compatible_with_scalars_only() {
        let s = ConflictStrategy::LastWriteWins;
        assert!(s.is_compatible_with(FieldType::String));
        assert!(s.is_compatible_with(FieldType::Int));
        assert!(s.is_compatible_with(FieldType::Float));
        assert!(s.is_compatible_with(FieldType::Bool));
        assert!(s.is_compatible_with(FieldType::Bytes));
        assert!(s.is_compatible_with(FieldType::Reference));
        assert!(!s.is_compatible_with(FieldType::Collection));
    }

    #[test]
    fn server_authority_compatible_with_all_types() {
        let s = ConflictStrategy::ServerAuthority;
        for ft in [
            FieldType::String,
            FieldType::Int,
            FieldType::Float,
            FieldType::Bool,
            FieldType::Bytes,
            FieldType::Reference,
            FieldType::Collection,
        ] {
            assert!(
                s.is_compatible_with(ft),
                "ServerAuthority should accept {ft:?}"
            );
        }
    }

    #[test]
    fn custom_compatible_with_all_types() {
        let s = ConflictStrategy::Custom(CustomResolverId::new("merge"));
        for ft in [FieldType::String, FieldType::Int, FieldType::Collection] {
            assert!(s.is_compatible_with(ft));
        }
    }

    #[test]
    fn crdt_strategy_delegates_to_hint_compatibility() {
        let counter = ConflictStrategy::Crdt(CrdtKind::Counter);
        assert!(counter.is_compatible_with(FieldType::Int));
        assert!(!counter.is_compatible_with(FieldType::String));

        let orset = ConflictStrategy::Crdt(CrdtKind::OrSet);
        assert!(orset.is_compatible_with(FieldType::Collection));
        assert!(!orset.is_compatible_with(FieldType::Int));
    }

    #[test]
    fn crdt_kind_round_trips_with_hint() {
        for kind in [CrdtKind::LwwRegister, CrdtKind::OrSet, CrdtKind::Counter] {
            let hint: CrdtHint = kind.into();
            let back: CrdtKind = hint.into();
            assert_eq!(kind, back);
        }
    }

    #[test]
    fn from_crdt_hint_wraps_correctly() {
        assert_eq!(
            ConflictStrategy::from_crdt_hint(CrdtHint::Lww),
            ConflictStrategy::Crdt(CrdtKind::LwwRegister)
        );
        assert_eq!(
            ConflictStrategy::from_crdt_hint(CrdtHint::OrSet),
            ConflictStrategy::Crdt(CrdtKind::OrSet)
        );
        assert_eq!(
            ConflictStrategy::from_crdt_hint(CrdtHint::Counter),
            ConflictStrategy::Crdt(CrdtKind::Counter)
        );
    }

    #[test]
    fn custom_resolver_id_constructors() {
        let from_str: CustomResolverId = "merge".into();
        let from_string: CustomResolverId = String::from("merge").into();
        let from_new = CustomResolverId::new("merge");
        assert_eq!(from_str, from_string);
        assert_eq!(from_str, from_new);
        assert_eq!(from_str.as_str(), "merge");
        assert_eq!(from_str.to_string(), "merge");
    }

    #[test]
    fn serde_round_trip_strategy_variants() {
        let cases = vec![
            ConflictStrategy::Crdt(CrdtKind::LwwRegister),
            ConflictStrategy::Crdt(CrdtKind::OrSet),
            ConflictStrategy::Crdt(CrdtKind::Counter),
            ConflictStrategy::LastWriteWins,
            ConflictStrategy::ServerAuthority,
            ConflictStrategy::Custom(CustomResolverId::new("merge_prefs")),
        ];
        for s in cases {
            let json = serde_json::to_string(&s).expect("serialize");
            let decoded: ConflictStrategy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(s, decoded);
        }
    }
}
