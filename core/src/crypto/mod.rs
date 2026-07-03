//! Encryption at rest (F4) — AES-256-GCM sealing for storage-layer files.
//!
//! Every persistent artifact (WAL entries, SSTable blocks, manifest
//! records) can be wrapped in an authenticated-encryption envelope:
//!
//! ```text
//! [nonce: 12 bytes][ciphertext || tag: plaintext_len + 16 bytes]
//! ```
//!
//! A fresh random nonce is drawn per seal from the OS RNG, so the same
//! plaintext never produces the same ciphertext. GCM's authentication tag
//! doubles as an integrity check: any bit flip in the sealed bytes fails
//! `open` deterministically ([`CryptoError::Integrity`]).
//!
//! The key is supplied by the caller via
//! [`StorageConfig::encryption_key`](crate::StorageConfig) — key custody
//! (Keychain, Android Keystore, …) belongs to the platform bindings, not
//! to this crate.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

/// Key length in bytes (AES-256).
pub const KEY_LEN: usize = 32;

/// Nonce length in bytes (GCM standard, 96 bits).
pub const NONCE_LEN: usize = 12;

/// Authentication tag length in bytes.
pub const TAG_LEN: usize = 16;

/// Bytes added to a plaintext by [`Cipher::seal`]: nonce prefix + GCM tag.
pub const SEAL_OVERHEAD: usize = NONCE_LEN + TAG_LEN;

/// Errors from sealing or opening encrypted data.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// Ciphertext failed authentication — wrong key or corrupted bytes.
    #[error("ciphertext failed authentication (wrong key or corrupted data)")]
    Integrity,

    /// Sealed buffer is too short to contain a nonce and tag.
    #[error("sealed buffer too short: {len} bytes, need at least {min}", min = SEAL_OVERHEAD)]
    TooShort { len: usize },

    /// The OS random-number generator failed while drawing a nonce.
    #[error("failed to generate nonce: {0}")]
    Rng(String),
}

/// A 256-bit encryption key supplied by the caller.
///
/// `Debug` is intentionally redacted so the key can never leak through
/// logs or error messages containing a `StorageConfig`.
#[derive(Clone, PartialEq, Eq)]
pub struct EncryptionKey([u8; KEY_LEN]);

impl EncryptionKey {
    /// Wrap raw key bytes. The caller owns key generation and custody
    /// (platform keystores in the bindings).
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptionKey(<redacted>)")
    }
}

/// AES-256-GCM cipher shared by the WAL, SSTable, and manifest writers.
pub struct Cipher {
    aead: Aes256Gcm,
}

impl Cipher {
    /// Build a cipher from a caller-supplied key.
    pub fn new(key: &EncryptionKey) -> Self {
        Self {
            aead: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key.as_bytes())),
        }
    }

    /// Encrypt `plaintext` under a fresh random nonce.
    ///
    /// Returns `[nonce][ciphertext || tag]` — exactly
    /// `plaintext.len() + SEAL_OVERHEAD` bytes.
    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|e| CryptoError::Rng(e.to_string()))?;

        let ciphertext = self
            .aead
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(|_| CryptoError::Integrity)?;

        let mut sealed = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed)
    }

    /// Decrypt and authenticate a buffer produced by [`Cipher::seal`].
    ///
    /// Fails with [`CryptoError::Integrity`] if the bytes were produced
    /// under a different key or modified in any way.
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if sealed.len() < SEAL_OVERHEAD {
            return Err(CryptoError::TooShort { len: sealed.len() });
        }
        let (nonce, ciphertext) = sealed.split_at(NONCE_LEN);
        self.aead
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| CryptoError::Integrity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> EncryptionKey {
        EncryptionKey::from_bytes([byte; KEY_LEN])
    }

    #[test]
    fn seal_open_round_trip() {
        let cipher = Cipher::new(&key(0x42));
        let plaintext = b"the raft is always afloat";
        let sealed = cipher.seal(plaintext).unwrap();
        assert_eq!(sealed.len(), plaintext.len() + SEAL_OVERHEAD);
        assert_eq!(cipher.open(&sealed).unwrap(), plaintext);
    }

    #[test]
    fn sealed_bytes_do_not_contain_plaintext() {
        let cipher = Cipher::new(&key(0x42));
        let plaintext = b"super-secret-document-field";
        let sealed = cipher.seal(plaintext).unwrap();
        assert!(!sealed
            .windows(plaintext.len())
            .any(|w| w == plaintext.as_slice()));
    }

    #[test]
    fn same_plaintext_seals_differently_each_time() {
        let cipher = Cipher::new(&key(0x42));
        let a = cipher.seal(b"data").unwrap();
        let b = cipher.seal(b"data").unwrap();
        assert_ne!(a, b, "fresh nonce per seal must randomize ciphertext");
    }

    #[test]
    fn wrong_key_fails_authentication() {
        let sealed = Cipher::new(&key(0x01)).seal(b"data").unwrap();
        let result = Cipher::new(&key(0x02)).open(&sealed);
        assert!(matches!(result, Err(CryptoError::Integrity)));
    }

    #[test]
    fn tampered_ciphertext_fails_authentication() {
        let cipher = Cipher::new(&key(0x42));
        let mut sealed = cipher
            .seal(b"data that must not be silently altered")
            .unwrap();
        for idx in [0, NONCE_LEN, sealed.len() - 1] {
            sealed[idx] ^= 0xFF;
            assert!(matches!(cipher.open(&sealed), Err(CryptoError::Integrity)));
            sealed[idx] ^= 0xFF; // restore
        }
        // Sanity: untampered buffer still opens.
        assert!(cipher.open(&sealed).is_ok());
    }

    #[test]
    fn truncated_buffer_is_rejected() {
        let cipher = Cipher::new(&key(0x42));
        assert!(matches!(
            cipher.open(&[0u8; SEAL_OVERHEAD - 1]),
            Err(CryptoError::TooShort { .. })
        ));
    }

    #[test]
    fn empty_plaintext_round_trips() {
        let cipher = Cipher::new(&key(0x42));
        let sealed = cipher.seal(b"").unwrap();
        assert_eq!(sealed.len(), SEAL_OVERHEAD);
        assert_eq!(cipher.open(&sealed).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn key_debug_is_redacted() {
        let debug = format!("{:?}", key(0xAB));
        assert_eq!(debug, "EncryptionKey(<redacted>)");
        assert!(!debug.contains("171"), "no key bytes in Debug output");
    }
}
