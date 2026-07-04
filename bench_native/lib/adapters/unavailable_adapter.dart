import 'package:raft_bench/raft_bench_core.dart';

/// A placeholder adapter that always reports unavailable with a fixed reason.
/// Used so an engine that can't run headless in this sandbox (native lib
/// requires a network download) still appears in the report, honestly marked
/// skipped rather than silently dropped.
class UnavailableAdapter implements DbAdapter {
  UnavailableAdapter(this.name, this._reason, {this.durabilityNote = ''});

  @override
  final String name;
  final String _reason;

  @override
  String get version => 'not run here';

  @override
  final String durabilityNote;

  @override
  Future<Availability> probe() async => Availability.unavailable(_reason);

  @override
  Set<Workload> get supported => Workload.values.toSet();

  @override
  Future<void> openFresh(String dir) async {}
  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {}
  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {}
  @override
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks) async {}
  @override
  Future<int> pointReads(List<int> ids) async => 0;
  @override
  Future<int> readMany(List<int> ids) async => 0;
  @override
  bool get supportsCachedReads => false;
  @override
  Future<int> cachedPointReads(List<int> ids) async => 0;
  @override
  Future<int> iterateAll() async => 0;
  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async {}
  @override
  Future<void> bulkDelete(List<int> ids) async {}
  @override
  Future<void> close() async {}
}
