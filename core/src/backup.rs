//! Backup snapshot format — a consistent logical export of the KV store.
//!
//! A snapshot is a single self-contained file (X7):
//!
//! ```text
//! [magic: "RFTBKUP1" (8 bytes)]
//! [count: u64 BE]
//! count × [klen: u32 BE][key][vlen: u32 BE][value]
//! [crc32: u32 BE]   — over everything after the magic
//! ```
//!
//! The format is *logical*: it captures live key/value pairs, not SSTable
//! files, so a snapshot can be restored into an engine with a different
//! configuration (block size, encryption key, ...). Values are written in
//! plaintext — exporting from an encrypted database produces an
//! unencrypted backup file; protect it accordingly.
//!
//! Truncated or bit-flipped files are rejected by the trailing checksum.

use std::io::{self, Read, Write};

/// File magic — 8 bytes, versioned.
const MAGIC: &[u8; 8] = b"RFTBKUP1";

/// One key/value pair in a snapshot.
pub type SnapshotEntry = (Vec<u8>, Vec<u8>);

/// Errors reading or writing a backup snapshot.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("not a raft backup file (bad magic)")]
    BadMagic,

    #[error("backup file corrupt: {0}")]
    Corrupt(String),
}

/// Write a snapshot of `entries` to `w`.
pub fn write_snapshot<W: Write>(w: &mut W, entries: &[SnapshotEntry]) -> Result<(), BackupError> {
    let mut hasher = crc32fast::Hasher::new();
    w.write_all(MAGIC)?;

    let count = (entries.len() as u64).to_be_bytes();
    hasher.update(&count);
    w.write_all(&count)?;

    for (key, value) in entries {
        let klen = (key.len() as u32).to_be_bytes();
        let vlen = (value.len() as u32).to_be_bytes();
        for part in [&klen[..], key, &vlen[..], value] {
            hasher.update(part);
            w.write_all(part)?;
        }
    }

    w.write_all(&hasher.finalize().to_be_bytes())?;
    Ok(())
}

/// Read and verify a snapshot from `r`.
pub fn read_snapshot<R: Read>(r: &mut R) -> Result<Vec<SnapshotEntry>, BackupError> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)
        .map_err(|_| BackupError::BadMagic)?;
    if &magic != MAGIC {
        return Err(BackupError::BadMagic);
    }

    // Everything after the magic except the trailing crc is checksummed.
    let mut body = Vec::new();
    r.read_to_end(&mut body)?;
    if body.len() < 12 {
        return Err(BackupError::Corrupt("file too short".into()));
    }
    let crc_offset = body.len() - 4;
    let stored = u32::from_be_bytes(body[crc_offset..].try_into().unwrap());
    let computed = crc32fast::hash(&body[..crc_offset]);
    if stored != computed {
        return Err(BackupError::Corrupt(format!(
            "checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
        )));
    }

    let body = &body[..crc_offset];
    let count = u64::from_be_bytes(body[..8].try_into().unwrap());
    let mut pos = 8usize;
    let mut entries = Vec::new();

    let take = |pos: &mut usize, n: usize| -> Result<&[u8], BackupError> {
        let end = pos
            .checked_add(n)
            .filter(|&e| e <= body.len())
            .ok_or_else(|| BackupError::Corrupt("record overruns file".into()))?;
        let slice = &body[*pos..end];
        *pos = end;
        Ok(slice)
    };

    for _ in 0..count {
        let klen = u32::from_be_bytes(take(&mut pos, 4)?.try_into().unwrap()) as usize;
        let key = take(&mut pos, klen)?.to_vec();
        let vlen = u32::from_be_bytes(take(&mut pos, 4)?.try_into().unwrap()) as usize;
        let value = take(&mut pos, vlen)?.to_vec();
        entries.push((key, value));
    }
    if pos != body.len() {
        return Err(BackupError::Corrupt("trailing bytes after records".into()));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"alpha".to_vec(), b"1".to_vec()),
            (b"beta".to_vec(), Vec::new()),
            (b"gamma".to_vec(), vec![0xFF; 300]),
        ]
    }

    #[test]
    fn snapshot_round_trip() {
        let entries = sample();
        let mut buf = Vec::new();
        write_snapshot(&mut buf, &entries).unwrap();
        let decoded = read_snapshot(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn empty_snapshot_round_trip() {
        let mut buf = Vec::new();
        write_snapshot(&mut buf, &[]).unwrap();
        let decoded = read_snapshot(&mut buf.as_slice()).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = Vec::new();
        write_snapshot(&mut buf, &sample()).unwrap();
        buf[0] ^= 0xFF;
        assert!(matches!(
            read_snapshot(&mut buf.as_slice()),
            Err(BackupError::BadMagic)
        ));
    }

    #[test]
    fn bitflip_anywhere_in_body_rejected() {
        let entries = sample();
        let mut clean = Vec::new();
        write_snapshot(&mut clean, &entries).unwrap();
        for i in 8..clean.len() {
            let mut corrupted = clean.clone();
            corrupted[i] ^= 0x01;
            assert!(
                matches!(
                    read_snapshot(&mut corrupted.as_slice()),
                    Err(BackupError::Corrupt(_))
                ),
                "bitflip at byte {i} must be detected"
            );
        }
    }

    #[test]
    fn truncated_file_rejected() {
        let mut buf = Vec::new();
        write_snapshot(&mut buf, &sample()).unwrap();
        for cut in [buf.len() - 1, buf.len() / 2, 9] {
            let truncated = &buf[..cut];
            assert!(
                read_snapshot(&mut &truncated[..]).is_err(),
                "truncation to {cut} bytes must be detected"
            );
        }
    }
}
