import 'dart:convert';
import 'dart:typed_data';

/// What kind of mutation occurred.
enum MutationKind { insert, update, delete, resyncRequired }

/// Whether the mutation originated locally or arrived from a network peer.
enum MutationOrigin { local, remote }

/// A mutation notification emitted by [RaftDb.observeCollection].
///
/// The Rust core emits these as JSON over the FFI; Dart decodes them
/// into this class before yielding into the stream.
class MutationEvent {
  const MutationEvent({
    required this.collection,
    required this.docId,
    required this.mutationType,
    required this.origin,
  });

  /// Collection name that was mutated.
  final String collection;

  /// Document id within the collection.
  final int docId;

  /// What kind of change occurred.
  final MutationKind mutationType;

  /// Where the mutation came from.
  final MutationOrigin origin;

  /// Parse the JSON payload emitted by `rft_observe`.
  factory MutationEvent.fromJson(Map<String, dynamic> j) {
    MutationKind kind;
    switch (j['mutation_type']) {
      case 'Insert':
        kind = MutationKind.insert;
        break;
      case 'Update':
        kind = MutationKind.update;
        break;
      case 'Delete':
        kind = MutationKind.delete;
        break;
      case 'ResyncRequired':
        kind = MutationKind.resyncRequired;
        break;
      default:
        throw FormatException('Unknown mutation_type: ${j['mutation_type']}');
    }
    final origin = (j['origin'] == 'Remote')
        ? MutationOrigin.remote
        : MutationOrigin.local;
    return MutationEvent(
      collection: j['collection'] as String,
      docId: (j['doc_id'] as num).toInt(),
      mutationType: kind,
      origin: origin,
    );
  }

  @override
  String toString() =>
      'MutationEvent(collection: $collection, docId: $docId, type: $mutationType, origin: $origin)';
}

/// The diff between two consecutive live-query result sets.
///
/// Emitted by [RaftDb.observeQuery]. Each bucket holds raw JSON bytes
/// for the documents that were added, removed, or updated since the
/// previous tick. Decode each entry yourself with `jsonDecode(utf8.decode(...))`.
class QueryDiff {
  const QueryDiff({
    this.added = const [],
    this.removed = const [],
    this.updated = const [],
  });

  /// Documents present in the new result set but not the old.
  final List<Uint8List> added;

  /// Documents present in the old result set but not the new.
  final List<Uint8List> removed;

  /// Documents present in both but with changed field values.
  final List<Uint8List> updated;

  /// `true` if no bucket contains any documents.
  bool get isEmpty => added.isEmpty && removed.isEmpty && updated.isEmpty;

  /// Parse a live-query JSON payload into a [QueryDiff]. Each element
  /// is preserved as raw JSON bytes for one document.
  factory QueryDiff.fromJson(String json) {
    final obj = jsonDecode(json);
    if (obj is! Map<String, dynamic>) {
      throw const FormatException('QueryDiff JSON is not an object');
    }
    List<Uint8List> bucket(String key) {
      final list = obj[key];
      if (list is! List) return const [];
      return list
          .map((e) => Uint8List.fromList(utf8.encode(jsonEncode(e))))
          .toList(growable: false);
    }

    return QueryDiff(
      added: bucket('added'),
      removed: bucket('removed'),
      updated: bucket('updated'),
    );
  }

  @override
  String toString() =>
      'QueryDiff(added: ${added.length}, removed: ${removed.length}, updated: ${updated.length})';
}
