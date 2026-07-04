import 'dart:math';

/// A fixed-shape record every engine stores identically.
///
/// Modelled as a small document keyed by an integer primary key — the shape
/// Isar, ObjectBox, Realm, Hive and a SQLite table all represent natively,
/// so no engine is handicapped by an unnatural data model.
class BenchDoc {
  const BenchDoc({
    required this.id,
    required this.name,
    required this.score,
    required this.payload,
  });

  /// Primary key, 1..N.
  final int id;

  /// Indexed-ish text field, e.g. `user-00000042`.
  final String name;

  /// Numeric field, used by update/query workloads.
  final int score;

  /// Filler text so each record reaches a realistic on-disk size.
  final String payload;
}

/// Deterministic dataset generator — identical bytes for every engine and
/// every run given the same [BenchConfig].
class Dataset {
  Dataset(this.config) {
    final filler = List<String>.generate(
      config.payloadBytes,
      (i) => String.fromCharCode(97 + (i % 26)),
    ).join();
    docs = List<BenchDoc>.generate(config.recordCount, (i) {
      final id = i + 1;
      return BenchDoc(
        id: id,
        name: 'user-${id.toString().padLeft(8, '0')}',
        score: id,
        payload: filler,
      );
    }, growable: false);

    // A fixed prime-stride read order over the dataset (matches the Rust
    // harness's 7919-stride access pattern) so reads are neither purely
    // sequential nor cache-trivial.
    final rng = Random(config.seed);
    final n = config.recordCount;
    readOrder = List<int>.generate(config.readCount, (i) {
      final idx = (i * 7919) % n;
      return docs[idx].id;
    }, growable: false);
    // Shuffle a copy for durable-write id order to avoid sequential-append
    // advantages leaking into that group.
    durableSubset = docs.take(config.durableCount).toList()..shuffle(rng);
  }

  final BenchConfig config;
  late final List<BenchDoc> docs;
  late final List<int> readOrder;
  late final List<BenchDoc> durableSubset;
}

/// Tunable sizes for a benchmark run.
class BenchConfig {
  const BenchConfig({
    this.recordCount = 10000,
    this.durableCount = 500,
    this.readCount = 10000,
    this.payloadBytes = 100,
    this.writeSamples = 3,
    this.readSamples = 5,
    this.seed = 1,
  });

  /// N for bulk write / update / delete / iterate groups.
  final int recordCount;

  /// N for the durable single-commit write group (deliberately small — it is
  /// hardware-fsync-bound).
  final int durableCount;

  /// Number of point reads performed per read sample.
  final int readCount;

  /// Payload size per record, bytes.
  final int payloadBytes;

  /// Samples for write-family groups (fresh store per sample).
  final int writeSamples;

  /// Samples for read-family groups (store populated once).
  final int readSamples;

  /// Seed for deterministic dataset generation.
  final int seed;

  Map<String, dynamic> toJson() => {
    'recordCount': recordCount,
    'durableCount': durableCount,
    'readCount': readCount,
    'payloadBytes': payloadBytes,
    'writeSamples': writeSamples,
    'readSamples': readSamples,
    'seed': seed,
  };

  /// A quick, small config for smoke runs.
  static const smoke = BenchConfig(
    recordCount: 1000,
    durableCount: 100,
    readCount: 1000,
    writeSamples: 2,
    readSamples: 3,
  );
}

/// The workloads every adapter is measured against.
enum Workload {
  bulkWrite('bulk_write', 'Insert N records in one transaction', WorkloadKind.write),
  durableWrites('durable_writes', 'Insert records one durable commit each', WorkloadKind.write),
  concurrentDurable(
      'concurrent_durable',
      'Insert records one durable commit each, 4 concurrent writers',
      WorkloadKind.write),
  pointRead('point_read', 'Read records by primary key', WorkloadKind.read),
  pointReadCached(
      'point_read_cached',
      'Read records by primary key through a correctness-preserving cache',
      WorkloadKind.read),
  readMany('read_many', 'Fetch a batch of records by id in one call',
      WorkloadKind.read),
  iterateAll('iterate_all', 'Read every record (full scan)', WorkloadKind.read),
  bulkUpdate('bulk_update', 'Update every record in one transaction', WorkloadKind.write),
  bulkDelete('bulk_delete', 'Delete every record in one transaction', WorkloadKind.write);

  const Workload(this.id, this.description, this.kind);
  final String id;
  final String description;
  final WorkloadKind kind;
}

enum WorkloadKind { read, write }
