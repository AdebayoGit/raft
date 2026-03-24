use std::fs;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use raftdb::{StorageConfig, StorageEngine};

fn bench_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir()
        .join("raft_db_bench")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    dir
}

fn bench_config() -> StorageConfig {
    StorageConfig {
        memtable_size: 2 * 1024 * 1024, // 2 MiB
        block_size: 4096,
        ..StorageConfig::default()
    }
}

/// Benchmark: sequential writes of N keys.
fn sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");

    for &n in &[100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = bench_dir(&format!("seq_write_{n}"));
                    StorageEngine::open(&dir, bench_config()).unwrap()
                },
                |mut engine| {
                    for i in 0..n {
                        engine
                            .put(
                                format!("key-{i:08}").into_bytes(),
                                format!("value-{i:08}").into_bytes(),
                            )
                            .unwrap();
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

/// Benchmark: random point reads after loading N keys.
fn random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");

    for &n in &[100, 1_000, 10_000] {
        // Pre-load the database outside the timed section.
        let dir = bench_dir(&format!("rand_read_{n}"));
        {
            let mut engine = StorageEngine::open(&dir, bench_config()).unwrap();
            for i in 0..n {
                engine
                    .put(
                        format!("key-{i:08}").into_bytes(),
                        format!("value-{i:08}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let engine = StorageEngine::open(&dir, bench_config()).unwrap();
            // Simple deterministic "random" read pattern using a prime stride.
            b.iter(|| {
                let stride: usize = 7919;
                let mut idx = 0usize;
                for _ in 0..n {
                    idx = (idx + stride) % n;
                    let key = format!("key-{idx:08}");
                    let _ = engine.get(key.as_bytes());
                }
            });
        });
    }

    group.finish();
}

/// Benchmark: mixed workload — 70% reads, 20% writes, 10% deletes.
fn mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    let ops = 5_000usize;

    group.throughput(Throughput::Elements(ops as u64));
    group.bench_function("5000_ops", |b| {
        b.iter_batched(
            || {
                let dir = bench_dir("mixed");
                let mut engine = StorageEngine::open(&dir, bench_config()).unwrap();
                // Pre-seed 1000 keys.
                for i in 0..1000u32 {
                    engine
                        .put(
                            format!("key-{i:08}").into_bytes(),
                            format!("value-{i:08}").into_bytes(),
                        )
                        .unwrap();
                }
                engine
            },
            |mut engine| {
                for i in 0..ops {
                    let bucket = i % 10;
                    match bucket {
                        0 => {
                            // Delete (10%)
                            let key = format!("key-{:08}", i % 1000);
                            let _ = engine.delete(key.into_bytes());
                        }
                        1 | 2 => {
                            // Write (20%)
                            let key = format!("key-{:08}", 1000 + i);
                            let val = format!("newval-{i:08}");
                            let _ = engine.put(key.into_bytes(), val.into_bytes());
                        }
                        _ => {
                            // Read (70%)
                            let key = format!("key-{:08}", i % 1000);
                            let _ = engine.get(key.as_bytes());
                        }
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, sequential_writes, random_reads, mixed_workload);
criterion_main!(benches);
