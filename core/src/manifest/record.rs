use bytes::{Buf, BufMut};

use super::error::ManifestError;

/// Unique identifier for an SSTable across the lifetime of the database.
pub type TableId = u64;

/// Metadata about a single SSTable tracked by the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableMeta {
    /// Unique table identifier (monotonically increasing).
    pub id: TableId,
    /// Compaction level (0 = freshly flushed from memtable).
    pub level: u32,
    /// Smallest key in the table (inclusive).
    pub smallest_key: Vec<u8>,
    /// Largest key in the table (inclusive).
    pub largest_key: Vec<u8>,
    /// Number of key-value entries in the table.
    pub entry_count: u64,
    /// File size in bytes on disk.
    pub file_size: u64,
}

/// A single record in the manifest log.
///
/// Binary layout per record:
/// ```text
/// [tag: u8][payload ...][crc32: u32]
/// ```
///
/// Tags:
///   1 = AddTable  (full SSTableMeta)
///   2 = RemoveTable (table id)
///   3 = SetSequence (new sequence number)
///   4 = Snapshot   (sequence + vec of SSTableMeta — for compaction)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRecord {
    /// Register a new SSTable.
    AddTable(SSTableMeta),
    /// Remove an SSTable (after compaction).
    RemoveTable(TableId),
    /// Advance the DB-wide sequence number.
    SetSequence(u64),
    /// Full snapshot of the current version (sequence + all live tables).
    /// Written periodically to bound recovery time.
    Snapshot { sequence: u64, tables: Vec<SSTableMeta> },
}

// ── Tag constants ──

const TAG_ADD_TABLE: u8 = 1;
const TAG_REMOVE_TABLE: u8 = 2;
const TAG_SET_SEQUENCE: u8 = 3;
const TAG_SNAPSHOT: u8 = 4;

// ── SSTableMeta encoding ──

impl SSTableMeta {
    /// Encode into a buffer. Layout:
    /// `[id:u64][level:u32][smallest_key_len:u32][smallest_key]
    ///  [largest_key_len:u32][largest_key][entry_count:u64][file_size:u64]`
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u64(self.id);
        buf.put_u32(self.level);
        buf.put_u32(self.smallest_key.len() as u32);
        buf.put_slice(&self.smallest_key);
        buf.put_u32(self.largest_key.len() as u32);
        buf.put_slice(&self.largest_key);
        buf.put_u64(self.entry_count);
        buf.put_u64(self.file_size);
    }

    fn decode(buf: &mut &[u8]) -> Result<Self, String> {
        if buf.remaining() < 8 + 4 + 4 {
            return Err("truncated SSTableMeta header".into());
        }
        let id = buf.get_u64();
        let level = buf.get_u32();

        let sk_len = buf.get_u32() as usize;
        if buf.remaining() < sk_len {
            return Err("truncated smallest_key".into());
        }
        let mut smallest_key = vec![0u8; sk_len];
        buf.copy_to_slice(&mut smallest_key);

        if buf.remaining() < 4 {
            return Err("truncated largest_key_len".into());
        }
        let lk_len = buf.get_u32() as usize;
        if buf.remaining() < lk_len {
            return Err("truncated largest_key".into());
        }
        let mut largest_key = vec![0u8; lk_len];
        buf.copy_to_slice(&mut largest_key);

        if buf.remaining() < 16 {
            return Err("truncated entry_count/file_size".into());
        }
        let entry_count = buf.get_u64();
        let file_size = buf.get_u64();

        Ok(Self {
            id,
            level,
            smallest_key,
            largest_key,
            entry_count,
            file_size,
        })
    }
}

// ── ManifestRecord encoding ──

impl ManifestRecord {
    /// Encode this record into bytes (tag + payload + crc32).
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            Self::AddTable(meta) => {
                payload.push(TAG_ADD_TABLE);
                meta.encode(&mut payload);
            }
            Self::RemoveTable(id) => {
                payload.push(TAG_REMOVE_TABLE);
                payload.put_u64(*id);
            }
            Self::SetSequence(seq) => {
                payload.push(TAG_SET_SEQUENCE);
                payload.put_u64(*seq);
            }
            Self::Snapshot { sequence, tables } => {
                payload.push(TAG_SNAPSHOT);
                payload.put_u64(*sequence);
                payload.put_u32(tables.len() as u32);
                for t in tables {
                    t.encode(&mut payload);
                }
            }
        }

        // Length-prefix the payload so the decoder knows how much to read.
        let mut buf = Vec::with_capacity(4 + payload.len() + 4);
        buf.put_u32(payload.len() as u32);
        let checksum = crc32fast::hash(&payload);
        buf.extend_from_slice(&payload);
        buf.put_u32(checksum);
        buf
    }

    /// Decode one record from a byte cursor. Returns `None` at EOF.
    pub fn decode(buf: &mut &[u8], offset: u64) -> Result<Option<Self>, ManifestError> {
        // Need at least 4 bytes for payload_len.
        if buf.remaining() == 0 {
            return Ok(None);
        }
        if buf.remaining() < 4 {
            return Err(ManifestError::CorruptRecord {
                offset,
                reason: "truncated payload length".into(),
            });
        }

        let payload_len = buf.get_u32() as usize;

        // payload + crc32
        if buf.remaining() < payload_len + 4 {
            return Err(ManifestError::CorruptRecord {
                offset,
                reason: format!(
                    "need {} bytes, have {}",
                    payload_len + 4,
                    buf.remaining()
                ),
            });
        }

        let payload = &buf[..payload_len];
        let computed_crc = crc32fast::hash(payload);

        let mut payload_cursor: &[u8] = &buf[..payload_len];
        buf.advance(payload_len);
        let stored_crc = buf.get_u32();

        if stored_crc != computed_crc {
            return Err(ManifestError::ChecksumMismatch {
                offset,
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        if payload_cursor.is_empty() {
            return Err(ManifestError::CorruptRecord {
                offset,
                reason: "empty payload".into(),
            });
        }

        let tag = payload_cursor.get_u8();

        let record = match tag {
            TAG_ADD_TABLE => {
                let meta = SSTableMeta::decode(&mut payload_cursor).map_err(|reason| {
                    ManifestError::CorruptRecord { offset, reason }
                })?;
                Self::AddTable(meta)
            }
            TAG_REMOVE_TABLE => {
                if payload_cursor.remaining() < 8 {
                    return Err(ManifestError::CorruptRecord {
                        offset,
                        reason: "truncated table id".into(),
                    });
                }
                Self::RemoveTable(payload_cursor.get_u64())
            }
            TAG_SET_SEQUENCE => {
                if payload_cursor.remaining() < 8 {
                    return Err(ManifestError::CorruptRecord {
                        offset,
                        reason: "truncated sequence".into(),
                    });
                }
                Self::SetSequence(payload_cursor.get_u64())
            }
            TAG_SNAPSHOT => {
                if payload_cursor.remaining() < 12 {
                    return Err(ManifestError::CorruptRecord {
                        offset,
                        reason: "truncated snapshot header".into(),
                    });
                }
                let sequence = payload_cursor.get_u64();
                let count = payload_cursor.get_u32() as usize;
                let mut tables = Vec::with_capacity(count);
                for _ in 0..count {
                    let meta =
                        SSTableMeta::decode(&mut payload_cursor).map_err(|reason| {
                            ManifestError::CorruptRecord { offset, reason }
                        })?;
                    tables.push(meta);
                }
                Self::Snapshot { sequence, tables }
            }
            _ => return Err(ManifestError::UnknownTag(tag, offset)),
        };

        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta(id: TableId) -> SSTableMeta {
        SSTableMeta {
            id,
            level: 1,
            smallest_key: format!("key-{id:05}-first").into_bytes(),
            largest_key: format!("key-{id:05}-last").into_bytes(),
            entry_count: 100 * id,
            file_size: 4096 * id,
        }
    }

    #[test]
    fn add_table_round_trip() {
        let record = ManifestRecord::AddTable(sample_meta(42));
        let bytes = record.encode();
        let mut cursor: &[u8] = &bytes;
        let decoded = ManifestRecord::decode(&mut cursor, 0)
            .unwrap()
            .unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn remove_table_round_trip() {
        let record = ManifestRecord::RemoveTable(99);
        let bytes = record.encode();
        let mut cursor: &[u8] = &bytes;
        let decoded = ManifestRecord::decode(&mut cursor, 0)
            .unwrap()
            .unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn set_sequence_round_trip() {
        let record = ManifestRecord::SetSequence(123_456);
        let bytes = record.encode();
        let mut cursor: &[u8] = &bytes;
        let decoded = ManifestRecord::decode(&mut cursor, 0)
            .unwrap()
            .unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn snapshot_round_trip() {
        let record = ManifestRecord::Snapshot {
            sequence: 500,
            tables: vec![sample_meta(1), sample_meta(2), sample_meta(3)],
        };
        let bytes = record.encode();
        let mut cursor: &[u8] = &bytes;
        let decoded = ManifestRecord::decode(&mut cursor, 0)
            .unwrap()
            .unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn snapshot_empty_tables_round_trip() {
        let record = ManifestRecord::Snapshot {
            sequence: 0,
            tables: vec![],
        };
        let bytes = record.encode();
        let mut cursor: &[u8] = &bytes;
        let decoded = ManifestRecord::decode(&mut cursor, 0)
            .unwrap()
            .unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn decode_empty_returns_none() {
        let mut cursor: &[u8] = &[];
        let result = ManifestRecord::decode(&mut cursor, 0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_detects_corruption() {
        let record = ManifestRecord::SetSequence(42);
        let mut bytes = record.encode();
        // Flip a byte inside the payload.
        bytes[5] ^= 0xFF;
        let mut cursor: &[u8] = &bytes;
        let result = ManifestRecord::decode(&mut cursor, 0);
        assert!(matches!(
            result,
            Err(ManifestError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn decode_truncated_returns_error() {
        let record = ManifestRecord::AddTable(sample_meta(1));
        let bytes = record.encode();
        let truncated = &bytes[..bytes.len() / 2];
        let mut cursor: &[u8] = truncated;
        let result = ManifestRecord::decode(&mut cursor, 0);
        assert!(result.is_err());
    }

    #[test]
    fn multiple_records_sequential() {
        let records = vec![
            ManifestRecord::SetSequence(1),
            ManifestRecord::AddTable(sample_meta(10)),
            ManifestRecord::AddTable(sample_meta(20)),
            ManifestRecord::RemoveTable(10),
        ];

        let mut all_bytes = Vec::new();
        for r in &records {
            all_bytes.extend_from_slice(&r.encode());
        }

        let mut cursor: &[u8] = &all_bytes;
        let mut decoded = Vec::new();
        while let Some(r) = ManifestRecord::decode(&mut cursor, 0).unwrap() {
            decoded.push(r);
        }

        assert_eq!(decoded, records);
    }
}
