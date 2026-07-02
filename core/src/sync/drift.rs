//! HLC physical-drift validation for the merge surface.
//!
//! An incoming timestamp whose physical component is far ahead of the
//! local wall clock would, once merged, pin the local HLC into the
//! future: every subsequent local write inherits the poisoned physical
//! component (the HLC never regresses). To contain a peer with a broken
//! clock, the merge surface rejects timestamps that lead the local
//! clock by more than a configurable bound before they enter the log.
//!
//! Timestamps in the past (or up to the bound in the future) are always
//! accepted — lateness is normal for offline-first peers; only forward
//! drift is dangerous.

use crate::wal::HlcTimestamp;

/// Default maximum tolerated forward drift: 5 minutes.
pub const DEFAULT_MAX_DRIFT_MS: u64 = 5 * 60 * 1000;

/// An incoming timestamp led the local clock by more than the bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "remote timestamp {remote_physical_ms} ms is {drift_ms} ms ahead of \
     local clock {local_now_ms} ms (bound: {max_drift_ms} ms)"
)]
pub struct DriftError {
    /// Physical component of the rejected timestamp.
    pub remote_physical_ms: u64,
    /// Local wall clock at validation time.
    pub local_now_ms: u64,
    /// How far ahead the remote timestamp was.
    pub drift_ms: u64,
    /// The configured bound that was exceeded.
    pub max_drift_ms: u64,
}

/// Validate an incoming timestamp against the local wall clock.
///
/// Accepts any timestamp at or behind `local_now_ms + max_drift_ms`;
/// rejects timestamps further in the future. Callers on the merge
/// surface must reject the associated mutation (or ask the peer to
/// re-stamp) rather than merging it.
///
/// # Errors
///
/// Returns [`DriftError`] when
/// `incoming.physical > local_now_ms + max_drift_ms`.
pub fn validate_drift(
    incoming: HlcTimestamp,
    local_now_ms: u64,
    max_drift_ms: u64,
) -> Result<(), DriftError> {
    let drift_ms = incoming.physical.saturating_sub(local_now_ms);
    if drift_ms > max_drift_ms {
        return Err(DriftError {
            remote_physical_ms: incoming.physical,
            local_now_ms,
            drift_ms,
            max_drift_ms,
        });
    }
    Ok(())
}

/// Validate an incoming timestamp against the current system clock
/// using the given bound.
///
/// Convenience wrapper over [`validate_drift`] for callers that don't
/// carry their own clock reading.
///
/// # Errors
///
/// Returns [`DriftError`] when the timestamp exceeds the bound.
pub fn validate_drift_now(incoming: HlcTimestamp, max_drift_ms: u64) -> Result<(), DriftError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    validate_drift(incoming, now_ms, max_drift_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(physical: u64) -> HlcTimestamp {
        HlcTimestamp::new(physical, 0)
    }

    const NOW: u64 = 1_000_000_000;

    #[test]
    fn past_timestamp_is_accepted() {
        assert!(validate_drift(ts(NOW - 86_400_000), NOW, DEFAULT_MAX_DRIFT_MS).is_ok());
    }

    #[test]
    fn present_timestamp_is_accepted() {
        assert!(validate_drift(ts(NOW), NOW, DEFAULT_MAX_DRIFT_MS).is_ok());
    }

    #[test]
    fn drift_exactly_at_bound_is_accepted() {
        assert!(validate_drift(ts(NOW + DEFAULT_MAX_DRIFT_MS), NOW, DEFAULT_MAX_DRIFT_MS).is_ok());
    }

    #[test]
    fn drift_one_ms_beyond_bound_is_rejected() {
        let err = validate_drift(
            ts(NOW + DEFAULT_MAX_DRIFT_MS + 1),
            NOW,
            DEFAULT_MAX_DRIFT_MS,
        )
        .unwrap_err();
        assert_eq!(err.drift_ms, DEFAULT_MAX_DRIFT_MS + 1);
        assert_eq!(err.max_drift_ms, DEFAULT_MAX_DRIFT_MS);
        assert_eq!(err.local_now_ms, NOW);
    }

    #[test]
    fn far_future_timestamp_is_rejected() {
        assert!(validate_drift(ts(u64::MAX), NOW, DEFAULT_MAX_DRIFT_MS).is_err());
    }

    #[test]
    fn zero_bound_rejects_any_future_timestamp() {
        assert!(validate_drift(ts(NOW + 1), NOW, 0).is_err());
        assert!(validate_drift(ts(NOW), NOW, 0).is_ok());
    }

    #[test]
    fn logical_component_does_not_affect_drift() {
        let incoming = HlcTimestamp::new(NOW + DEFAULT_MAX_DRIFT_MS, u16::MAX);
        assert!(validate_drift(incoming, NOW, DEFAULT_MAX_DRIFT_MS).is_ok());
    }

    #[test]
    fn error_message_reports_context() {
        let err = validate_drift(ts(NOW + 600_000), NOW, DEFAULT_MAX_DRIFT_MS).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("600000 ms ahead"));
        assert!(msg.contains("bound: 300000 ms"));
    }

    #[test]
    fn validate_drift_now_accepts_recent_timestamp() {
        assert!(validate_drift_now(ts(0), DEFAULT_MAX_DRIFT_MS).is_ok());
        assert!(validate_drift_now(ts(u64::MAX), DEFAULT_MAX_DRIFT_MS).is_err());
    }
}
