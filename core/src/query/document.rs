//! Document model and storage trait.
//!
//! A [`Document`] is a named-field bag attached to a [`DocId`]. The
//! [`DocumentStore`] trait abstracts the backing storage so the query
//! engine can be tested without touching disk.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::index::DocId;

/// A dynamically-typed field value.
///
/// Kept intentionally simple — richer types (e.g. nested documents) can be
/// added later without breaking the query interface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Null,
}

impl Value {
    /// Encode the value to a byte representation suitable for index keys.
    ///
    /// Every encoding starts with a one-byte type tag so values of
    /// different types can never collide (`Null` vs `""` vs `0` vs
    /// `false`). Within a type, byte ordering matches value ordering:
    /// numeric types are big-endian with sign adjustments, strings and
    /// bytes are used as-is after the tag.
    pub fn to_index_bytes(&self) -> Vec<u8> {
        const TAG_NULL: u8 = 0x00;
        const TAG_BOOL: u8 = 0x01;
        const TAG_INT: u8 = 0x02;
        const TAG_FLOAT: u8 = 0x03;
        const TAG_STRING: u8 = 0x04;
        const TAG_BYTES: u8 = 0x05;

        match self {
            Value::Null => vec![TAG_NULL],
            Value::Bool(b) => vec![TAG_BOOL, u8::from(*b)],
            // Flip the sign bit so that signed integers sort correctly in
            // unsigned byte order.
            Value::Int(n) => {
                let unsigned = (*n as u64) ^ (1u64 << 63);
                let mut out = Vec::with_capacity(9);
                out.push(TAG_INT);
                out.extend_from_slice(&unsigned.to_be_bytes());
                out
            }
            Value::Float(f) => {
                let bits = f.to_bits();
                // IEEE 754 trick: if the sign bit is set, flip all bits;
                // otherwise flip only the sign bit. This gives correct
                // unsigned byte ordering for all finite floats.
                let sortable = if bits >> 63 == 1 {
                    !bits
                } else {
                    bits ^ (1u64 << 63)
                };
                let mut out = Vec::with_capacity(9);
                out.push(TAG_FLOAT);
                out.extend_from_slice(&sortable.to_be_bytes());
                out
            }
            Value::String(s) => {
                let mut out = Vec::with_capacity(1 + s.len());
                out.push(TAG_STRING);
                out.extend_from_slice(s.as_bytes());
                out
            }
            Value::Bytes(b) => {
                let mut out = Vec::with_capacity(1 + b.len());
                out.push(TAG_BYTES);
                out.extend_from_slice(b);
                out
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

/// A document: a unique ID plus a map of field names to values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    pub id: DocId,
    pub fields: HashMap<String, Value>,
}

impl Document {
    pub fn new(id: DocId) -> Self {
        Self {
            id,
            fields: HashMap::new(),
        }
    }

    pub fn with_field(mut self, name: impl Into<String>, value: Value) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    pub fn get(&self, field: &str) -> Option<&Value> {
        self.fields.get(field)
    }
}

/// Trait abstracting the document storage layer.
///
/// The query engine programs against this trait so it can be tested with
/// in-memory stores and later wired to the real storage engine.
pub trait DocumentStore {
    /// Fetch a single document by ID. Returns `None` if not found.
    fn get_document(&self, id: DocId) -> Option<Document>;

    /// Return all document IDs in the collection (unordered).
    fn all_doc_ids(&self) -> Vec<DocId>;

    /// Return all documents in the collection (unordered).
    fn all_documents(&self) -> Vec<Document> {
        self.all_doc_ids()
            .into_iter()
            .filter_map(|id| self.get_document(id))
            .collect()
    }

    /// Total number of documents.
    fn count(&self) -> usize {
        self.all_doc_ids().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_ordering_ints() {
        assert!(Value::Int(1) < Value::Int(2));
        assert!(Value::Int(-5) < Value::Int(0));
    }

    #[test]
    fn value_ordering_strings() {
        assert!(Value::String("apple".into()) < Value::String("banana".into()));
    }

    #[test]
    fn value_ordering_cross_type_returns_none() {
        assert_eq!(Value::Int(1).partial_cmp(&Value::String("a".into())), None);
    }

    #[test]
    fn int_index_bytes_preserve_order() {
        let neg = Value::Int(-100).to_index_bytes();
        let zero = Value::Int(0).to_index_bytes();
        let pos = Value::Int(100).to_index_bytes();
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn float_index_bytes_preserve_order() {
        let neg = Value::Float(-1.5).to_index_bytes();
        let zero = Value::Float(0.0).to_index_bytes();
        let pos = Value::Float(1.5).to_index_bytes();
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn index_bytes_distinguish_null_empty_string_zero_false() {
        let keys = [
            Value::Null.to_index_bytes(),
            Value::String(String::new()).to_index_bytes(),
            Value::Int(0).to_index_bytes(),
            Value::Bool(false).to_index_bytes(),
            Value::Bytes(Vec::new()).to_index_bytes(),
            Value::Float(0.0).to_index_bytes(),
        ];
        for (i, a) in keys.iter().enumerate() {
            for (j, b) in keys.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "index keys {i} and {j} must not collide");
                }
            }
        }
    }

    #[test]
    fn index_bytes_distinguish_string_from_equal_bytes() {
        let s = Value::String("abc".into()).to_index_bytes();
        let b = Value::Bytes(b"abc".to_vec()).to_index_bytes();
        assert_ne!(s, b);
    }

    #[test]
    fn index_bytes_distinguish_bool_from_small_bytes() {
        assert_ne!(
            Value::Bool(true).to_index_bytes(),
            Value::Bytes(vec![1]).to_index_bytes()
        );
    }

    #[test]
    fn string_index_bytes_preserve_order() {
        let a = Value::String("apple".into()).to_index_bytes();
        let b = Value::String("banana".into()).to_index_bytes();
        assert!(a < b);
    }

    #[test]
    fn document_builder() {
        let doc = Document::new(DocId(1))
            .with_field("name", Value::String("Alice".into()))
            .with_field("age", Value::Int(30));

        assert_eq!(doc.id, DocId(1));
        assert_eq!(doc.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(doc.get("age"), Some(&Value::Int(30)));
        assert_eq!(doc.get("missing"), None);
    }
}
