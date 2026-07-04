//! Compact binary document codec for the FFI hot path.
//!
//! JSON at the boundary costs more than the engine itself (measured: ~67%
//! of a 10k-doc bulk write is `jsonEncode`/`serde_json` time). This codec
//! replaces it for the batch APIs with a hand-rolled little-endian format —
//! no new dependencies, no reflection, strict bounds checks.
//!
//! # Wire format (v1, all little-endian)
//!
//! ```text
//! batch  := u32 doc_count, doc_count × (u32 doc_len, doc)
//! doc    := u64 id, u16 field_count, field_count × field
//! field  := u8 name_len, name bytes (UTF-8), u8 tag, payload
//! tag    := 0 Null | 1 Bool (u8) | 2 Int (i64) | 3 Float (f64)
//!         | 4 String (u32 len + UTF-8) | 5 Bytes (u32 len + raw)
//! ```
//!
//! Decoding never trusts a declared length: every read is bounds-checked
//! against the remaining input, so a corrupt or malicious prefix cannot
//! force an out-of-range read or an oversized allocation.

use std::collections::HashMap;

use crate::index::DocId;
use crate::query::{Document, Value};

/// Per-document size cap — matches `ffi::RFT_MAX_DOC_JSON_LEN` so both
/// boundary formats enforce the same limit.
pub(crate) const MAX_DOC_LEN: usize = 16 * 1024 * 1024;

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_BYTES: u8 = 5;

/// Decode failure — mapped to `RftError::InvalidJson` at the FFI boundary
/// (same class of error: malformed document payload).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CodecError;

/// Encode one document, appending to `out`.
pub(crate) fn encode_doc(doc: &Document, out: &mut Vec<u8>) {
    out.extend_from_slice(&doc.id.0.to_le_bytes());
    out.extend_from_slice(&(doc.fields.len() as u16).to_le_bytes());
    for (name, value) in &doc.fields {
        let name_bytes = name.as_bytes();
        debug_assert!(name_bytes.len() <= u8::MAX as usize);
        out.push(name_bytes.len().min(u8::MAX as usize) as u8);
        out.extend_from_slice(&name_bytes[..name_bytes.len().min(u8::MAX as usize)]);
        match value {
            Value::Null => out.push(TAG_NULL),
            Value::Bool(b) => {
                out.push(TAG_BOOL);
                out.push(*b as u8);
            }
            Value::Int(i) => {
                out.push(TAG_INT);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                out.push(TAG_FLOAT);
                out.extend_from_slice(&f.to_le_bytes());
            }
            Value::String(s) => {
                out.push(TAG_STRING);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Value::Bytes(b) => {
                out.push(TAG_BYTES);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
    }
}

/// Encode a batch of documents (scan output / put_many input format).
/// Production scans stream via [`BatchEncoder`]; this eager form is the
/// test reference implementation.
#[cfg(test)]
pub(crate) fn encode_batch(docs: &[Document]) -> Vec<u8> {
    // Rough pre-size: header + per-doc framing + payload guess to avoid
    // repeated growth on large scans.
    let mut out = Vec::with_capacity(8 + docs.len() * 64);
    out.extend_from_slice(&(docs.len() as u32).to_le_bytes());
    let mut scratch = Vec::new();
    for doc in docs {
        scratch.clear();
        encode_doc(doc, &mut scratch);
        out.extend_from_slice(&(scratch.len() as u32).to_le_bytes());
        out.extend_from_slice(&scratch);
    }
    out
}

/// Incremental batch encoder for streaming scans: documents are appended
/// in one pass and the count header is patched at the end, so a scan can
/// encode straight out of the collection's in-memory state — no
/// intermediate `Vec<Document>`, no per-document clones.
pub(crate) struct BatchEncoder {
    out: Vec<u8>,
    count: u32,
    scratch: Vec<u8>,
}

impl BatchEncoder {
    pub(crate) fn new() -> Self {
        Self {
            out: vec![0u8; 4], // count header, patched in finish()
            count: 0,
            scratch: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, doc: &Document) {
        self.scratch.clear();
        encode_doc(doc, &mut self.scratch);
        self.out
            .extend_from_slice(&(self.scratch.len() as u32).to_le_bytes());
        self.out.extend_from_slice(&self.scratch);
        self.count += 1;
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.out[..4].copy_from_slice(&self.count.to_le_bytes());
        self.out
    }
}

/// Bounds-checked little-endian reader over an input slice.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.pos.checked_add(n).ok_or(CodecError)?;
        if end > self.buf.len() {
            return Err(CodecError);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, CodecError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Decode one document from exactly `buf` (trailing bytes are an error).
pub(crate) fn decode_doc(buf: &[u8]) -> Result<Document, CodecError> {
    if buf.len() > MAX_DOC_LEN {
        return Err(CodecError);
    }
    let mut r = Reader::new(buf);
    let id = r.u64()?;
    let field_count = r.u16()? as usize;
    let mut fields = HashMap::with_capacity(field_count);
    for _ in 0..field_count {
        let name_len = r.u8()? as usize;
        let name = std::str::from_utf8(r.take(name_len)?)
            .map_err(|_| CodecError)?
            .to_owned();
        let value = match r.u8()? {
            TAG_NULL => Value::Null,
            TAG_BOOL => Value::Bool(r.u8()? != 0),
            TAG_INT => Value::Int(r.i64()?),
            TAG_FLOAT => Value::Float(r.f64()?),
            TAG_STRING => {
                let len = r.u32()? as usize;
                Value::String(
                    std::str::from_utf8(r.take(len)?)
                        .map_err(|_| CodecError)?
                        .to_owned(),
                )
            }
            TAG_BYTES => {
                let len = r.u32()? as usize;
                Value::Bytes(r.take(len)?.to_vec())
            }
            _ => return Err(CodecError),
        };
        fields.insert(name, value);
    }
    if !r.is_empty() {
        return Err(CodecError);
    }
    Ok(Document {
        id: DocId(id),
        fields,
    })
}

/// Decode a batch, returning each document together with the byte range of
/// its encoded frame inside `buf`. Callers that persist documents in this
/// same codec can slice the input instead of re-encoding — the write path's
/// bytes-in-hand fast path. Every document is fully validated before any is
/// returned.
pub(crate) fn decode_batch_spans(
    buf: &[u8],
) -> Result<Vec<(Document, std::ops::Range<usize>)>, CodecError> {
    let mut r = Reader::new(buf);
    let count = r.u32()? as usize;
    let max_possible = buf.len().saturating_sub(4) / 14 + 1;
    let mut docs = Vec::with_capacity(count.min(max_possible));
    for _ in 0..count {
        let len = r.u32()? as usize;
        if len > MAX_DOC_LEN {
            return Err(CodecError);
        }
        let start = r.pos;
        let frame = r.take(len)?;
        docs.push((decode_doc(frame)?, start..start + len));
    }
    if !r.is_empty() {
        return Err(CodecError);
    }
    Ok(docs)
}

/// Decode a batch. Every document is fully validated before any is
/// returned, so callers can apply all-or-nothing semantics. Production
/// paths use [`decode_batch_spans`]; this form is the test reference.
#[cfg(test)]
pub(crate) fn decode_batch(buf: &[u8]) -> Result<Vec<Document>, CodecError> {
    let mut r = Reader::new(buf);
    let count = r.u32()? as usize;
    // Cap the pre-allocation by what the input could physically hold
    // (empty doc frame = 4-byte len + 10-byte doc) so a lying header
    // cannot force a huge allocation.
    let max_possible = buf.len().saturating_sub(4) / 14 + 1;
    let mut docs = Vec::with_capacity(count.min(max_possible));
    for _ in 0..count {
        let len = r.u32()? as usize;
        if len > MAX_DOC_LEN {
            return Err(CodecError);
        }
        docs.push(decode_doc(r.take(len)?)?);
    }
    if !r.is_empty() {
        return Err(CodecError);
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: u64) -> Document {
        let mut fields = HashMap::new();
        fields.insert("name".into(), Value::String(format!("user-{id}")));
        fields.insert("score".into(), Value::Int(id as i64));
        fields.insert("ratio".into(), Value::Float(0.5));
        fields.insert("active".into(), Value::Bool(true));
        fields.insert("blob".into(), Value::Bytes(vec![1, 2, 3]));
        fields.insert("gone".into(), Value::Null);
        Document {
            id: DocId(id),
            fields,
        }
    }

    #[test]
    fn doc_roundtrip_preserves_all_value_types() {
        let original = doc(42);
        let mut buf = Vec::new();
        encode_doc(&original, &mut buf);
        let decoded = decode_doc(&buf).unwrap();
        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn batch_roundtrip() {
        let docs: Vec<_> = (1..=100).map(doc).collect();
        let buf = encode_batch(&docs);
        let decoded = decode_batch(&buf).unwrap();
        assert_eq!(decoded.len(), 100);
        assert_eq!(decoded[0].id, DocId(1));
        assert_eq!(decoded[99].fields, docs[99].fields);
    }

    #[test]
    fn streamed_encoder_matches_encode_batch() {
        let docs: Vec<_> = (1..=25).map(doc).collect();
        let mut enc = BatchEncoder::new();
        for d in &docs {
            enc.push(d);
        }
        assert_eq!(enc.finish(), encode_batch(&docs));
    }

    #[test]
    fn empty_batch_roundtrip() {
        let buf = encode_batch(&[]);
        assert_eq!(decode_batch(&buf).unwrap().len(), 0);
    }

    #[test]
    fn batch_spans_slice_back_to_identical_docs() {
        let docs: Vec<_> = (1..=10).map(doc).collect();
        let buf = encode_batch(&docs);
        let spans = decode_batch_spans(&buf).unwrap();
        assert_eq!(spans.len(), 10);
        for (decoded, range) in &spans {
            // Re-decoding the sliced frame yields the same document.
            let again = decode_doc(&buf[range.clone()]).unwrap();
            assert_eq!(again.id, decoded.id);
            assert_eq!(again.fields, decoded.fields);
        }
    }

    #[test]
    fn truncated_doc_is_rejected() {
        let mut buf = Vec::new();
        encode_doc(&doc(1), &mut buf);
        for cut in [0, 1, 8, 9, buf.len() - 1] {
            assert_eq!(decode_doc(&buf[..cut]), Err(CodecError), "cut at {cut}");
        }
    }

    #[test]
    fn trailing_garbage_is_rejected() {
        let mut buf = Vec::new();
        encode_doc(&doc(1), &mut buf);
        buf.push(0xFF);
        assert_eq!(decode_doc(&buf), Err(CodecError));
    }

    #[test]
    fn lying_string_length_is_rejected() {
        let mut buf = Vec::new();
        // id + 1 field: name "x", String with declared len 1000 but 2 bytes.
        buf.extend_from_slice(&7u64.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(1);
        buf.push(b'x');
        buf.push(TAG_STRING);
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(b"hi");
        assert_eq!(decode_doc(&buf), Err(CodecError));
    }

    #[test]
    fn lying_batch_count_is_rejected_without_huge_alloc() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // absurd count
        assert_eq!(decode_batch(&buf), Err(CodecError));
    }

    #[test]
    fn unknown_tag_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(1);
        buf.push(b'x');
        buf.push(99); // unknown tag
        assert_eq!(decode_doc(&buf), Err(CodecError));
    }

    #[test]
    fn invalid_utf8_name_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(2);
        buf.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8 name
        buf.push(TAG_NULL);
        assert_eq!(decode_doc(&buf), Err(CodecError));
    }

    #[test]
    fn oversized_doc_is_rejected() {
        // decode_doc: input larger than the cap fails fast.
        let big = vec![0u8; MAX_DOC_LEN + 1];
        assert_eq!(decode_doc(&big), Err(CodecError));
    }
}
