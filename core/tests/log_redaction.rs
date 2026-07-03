//! Log-output audit (X5): the `tracing` instrumentation must never emit
//! user data — keys, values, or document contents — at *any* level.
//!
//! The test drives every instrumented path (open, WAL recovery, memtable
//! flush, compaction) with sentinel-laden keys and values while capturing
//! all log output at TRACE, then asserts the sentinels never appear.

use std::io::Write;
use std::sync::{Arc, Mutex};

use raftdb::{StorageConfig, StorageEngine};

const KEY_SENTINEL: &str = "SECRET-KEY-cafebabe";
const VALUE_SENTINEL: &str = "SECRET-VALUE-deadbeef";

/// A `MakeWriter` that captures everything into a shared buffer.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn small_config() -> StorageConfig {
    StorageConfig {
        // Tiny memtable so a handful of puts force flushes.
        memtable_size: 512,
        ..StorageConfig::default()
    }
}

#[test]
fn logs_never_contain_keys_or_values() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(capture.clone())
        .with_ansi(false)
        .finish();

    let dir = std::env::temp_dir().join(format!(
        "raft-log-redaction-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    tracing::subscriber::with_default(subscriber, || {
        // Open (logs the path), write sentinel data across enough puts to
        // force memtable flushes, then compact and reopen (WAL recovery).
        let mut engine = StorageEngine::open(&dir, small_config()).unwrap();
        for i in 0..64 {
            let key = format!("{KEY_SENTINEL}-{i}").into_bytes();
            let value = format!("{VALUE_SENTINEL}-{i}").into_bytes();
            engine.put(key, value).unwrap();
        }
        engine.flush().unwrap();
        engine.compact().unwrap();
        drop(engine);

        // Reopen to exercise WAL recovery logging.
        let engine = StorageEngine::open(&dir, small_config()).unwrap();
        drop(engine);
    });

    let logs = capture.contents();
    assert!(
        !logs.is_empty(),
        "expected the instrumented paths to emit log output"
    );
    assert!(
        !logs.contains("SECRET-KEY"),
        "log output leaked a key: {logs}"
    );
    assert!(
        !logs.contains("SECRET-VALUE"),
        "log output leaked a value: {logs}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
