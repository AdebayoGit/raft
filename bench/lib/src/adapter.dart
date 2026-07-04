import 'model.dart';

/// Result of probing whether an engine can run in this environment.
class Availability {
  const Availability.available() : reason = null;
  const Availability.unavailable(this.reason);

  /// Non-null when the engine cannot run here; the reason is recorded in the
  /// report so a skipped engine is never confused with a fast one.
  final String? reason;

  bool get isAvailable => reason == null;
}

/// Common interface every database adapter implements. The harness drives
/// each engine only through these methods, so all engines run identical
/// logical workloads over the identical dataset.
///
/// Lifecycle per write-family sample: [probe] once, then repeatedly
/// [openFresh] → op → [close]. For read-family groups the store is populated
/// once (via [openFresh] + [bulkWrite]) then read [BenchConfig.readSamples]
/// times before [close].
abstract class DbAdapter {
  /// Display name, e.g. `raft-db` or `SQLite (sqflite_ffi)`.
  String get name;

  /// Engine/package version if known, else `unknown`.
  String get version;

  /// One-line, honest description of this engine's write durability under the
  /// settings the harness uses. Printed alongside results so readers can
  /// judge whether a write comparison is apples-to-apples.
  String get durabilityNote;

  /// The workloads this adapter can perform. Anything omitted is recorded as
  /// `N/A` rather than silently skipped.
  Set<Workload> get supported => Workload.values.toSet();

  /// Probe the environment. Return [Availability.unavailable] with a reason to
  /// skip cleanly (e.g. native library not installed on this host).
  Future<Availability> probe();

  /// Open a brand-new, empty store rooted under [dir] (a fresh directory the
  /// harness owns and deletes). Must leave the store empty.
  Future<void> openFresh(String dir);

  /// Insert every doc in [docs] within a single transaction / batch.
  Future<void> bulkWrite(List<BenchDoc> docs);

  /// Insert every doc in [docs], each in its own durable commit.
  Future<void> durableWrites(List<BenchDoc> docs);

  /// Insert every doc in [docs], each in its own durable commit, issued by
  /// [concurrency] concurrent clients (the docs are pre-split into chunks).
  /// This is the real-app write pattern — UI isolate plus background workers
  /// committing simultaneously — and is where engines with group commit pull
  /// ahead of engines with a global writer lock.
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks);

  /// Point-read each id in [ids]; return how many were found (should equal
  /// ids.length).
  Future<int> pointReads(List<int> ids);

  /// Fetch all of [ids] through the engine's batch-read API (one call /
  /// crossing where the engine supports it); return how many were found.
  Future<int> readMany(List<int> ids);

  /// Whether this engine offers a correctness-preserving cached read mode
  /// (invalidated on every write). Engines without one report the
  /// workload as unsupported rather than faking it.
  bool get supportsCachedReads => false;

  /// Point-read each id through the engine's cached read mode. Only
  /// called when [supportsCachedReads] is true.
  Future<int> cachedPointReads(List<int> ids) =>
      throw UnsupportedError('no cached read mode');

  /// Read every record in the store; return the count seen.
  Future<int> iterateAll();

  /// Update every doc in [docs] (by id) in a single transaction.
  Future<void> bulkUpdate(List<BenchDoc> docs);

  /// Delete every id in [ids] in a single transaction.
  Future<void> bulkDelete(List<int> ids);

  /// Close and release all resources for the current store.
  Future<void> close();
}
