import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:raft_db/raft_db.dart';
import 'package:raft_db/src/raft_doc.dart';

void main() {
  // RaftDb.open requires a real compiled native library, so runtime
  // integration tests live in integration_test/. These unit tests cover
  // the pure-Dart surface: exception formatting, error code mapping,
  // and the RaftDoc binary codec.

  group('RaftDbException', () {
    test('formats message without code', () {
      const ex = RaftDbException('something went wrong');
      expect(ex.toString(), 'RaftDbException: something went wrong');
    });

    test('formats message with code', () {
      const ex = RaftDbException('I/O error', code: 3);
      expect(ex.toString(), 'RaftDbException: I/O error (code 3)');
    });

    test('is an Exception', () {
      const ex = RaftDbException('oops');
      expect(ex, isA<Exception>());
    });
  });

  group('RaftDbException.fromCode', () {
    test('code 1 maps to null-pointer message', () {
      final ex = RaftDbException.fromCode(1);
      expect(ex.code, 1);
      expect(ex.message.toLowerCase(), contains('null'));
    });

    test('code 2 mentions UTF-8', () {
      final ex = RaftDbException.fromCode(2);
      expect(ex.message, contains('UTF-8'));
    });

    test('code 3 mentions I/O', () {
      final ex = RaftDbException.fromCode(3);
      expect(ex.message, contains('I/O'));
    });

    test('code 4 mentions not-found', () {
      final ex = RaftDbException.fromCode(4);
      expect(ex.message.toLowerCase(), contains('not found'));
    });

    test('code 5 mentions buffer', () {
      final ex = RaftDbException.fromCode(5);
      expect(ex.message.toLowerCase(), contains('buffer'));
    });

    test('code 6 mentions JSON', () {
      final ex = RaftDbException.fromCode(6);
      expect(ex.message, contains('JSON'));
    });

    test('code 7 mentions conflict', () {
      final ex = RaftDbException.fromCode(7);
      expect(ex.message.toLowerCase(), contains('conflict'));
    });

    test('code 8 mentions handle', () {
      final ex = RaftDbException.fromCode(8);
      expect(ex.message.toLowerCase(), contains('handle'));
    });

    test('code 9 mentions subscription', () {
      final ex = RaftDbException.fromCode(9);
      expect(ex.message.toLowerCase(), contains('subscription'));
    });

    test('code 10 mentions panic', () {
      final ex = RaftDbException.fromCode(10);
      expect(ex.message.toLowerCase(), contains('panic'));
    });

    test('code 11 mentions path', () {
      final ex = RaftDbException.fromCode(11);
      expect(ex.message.toLowerCase(), contains('path'));
    });

    test('code 12 mentions Dart API', () {
      final ex = RaftDbException.fromCode(12);
      expect(ex.message, contains('Dart API'));
    });

    test('code 13 mentions size cap', () {
      final ex = RaftDbException.fromCode(13);
      expect(ex.message.toLowerCase(), contains('size cap'));
    });

    test('code 14 mentions unsupported version', () {
      final ex = RaftDbException.fromCode(14);
      expect(ex.message.toLowerCase(), contains('unsupported'));
      expect(ex.message.toLowerCase(), contains('version'));
    });

    test('unknown code falls back to generic message including code', () {
      final ex = RaftDbException.fromCode(99);
      expect(ex.code, 99);
      expect(ex.message.toLowerCase(), contains('unknown'));
      expect(ex.message, contains('99'));
    });

    test('all known codes round-trip through code field', () {
      for (final code in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]) {
        final ex = RaftDbException.fromCode(code);
        expect(ex.code, code, reason: 'code $code should round-trip');
      }
    });
  });

  group('RaftDoc binary codec (pure-Dart)', () {
    test('writer/reader round-trips every field type', () {
      final bytes = RaftWire.encodeDoc(42, (w) {
        w
          ..string('name', 'Alice ✓ ünïcode')
          ..integer('age', -7)
          ..float('ratio', 0.25)
          ..boolean('active', true)
          ..bytes('blob', Uint8List.fromList([1, 2, 3]))
          ..nullField('gone');
      });
      final r = RaftWire.decodeDoc(
        bytes,
        ByteData.view(bytes.buffer),
        0,
        bytes.length,
      );
      expect(r.id, 42);
      expect(r.string('name'), 'Alice ✓ ünïcode');
      expect(r.integer('age'), -7);
      expect(r.float('ratio'), 0.25);
      expect(r.boolean('active'), isTrue);
      expect(r.bytes('blob'), [1, 2, 3]);
      expect(r.has('gone'), isTrue);
      expect(r.stringOrNull('gone'), isNull);
    });

    test('batch round-trips and preserves order', () {
      final batch = RaftWire.encodeBatch(
        3,
        (i, w) => w..integer('n', i * 10),
        (i) => i + 1,
      );
      final docs = RaftWire.decodeBatch(batch);
      expect(docs.map((d) => d.id), [1, 2, 3]);
      expect(docs.map((d) => d.integer('n')), [0, 10, 20]);
    });

    test('missing field throws a descriptive StateError, not silent zero', () {
      final bytes = RaftWire.encodeDoc(1, (w) => w.string('present', 'x'));
      final r = RaftWire.decodeDoc(
        bytes,
        ByteData.view(bytes.buffer),
        0,
        bytes.length,
      );
      expect(
        () => r.string('absnt'),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('absnt'),
          ),
        ),
      );
    });

    test('wrong field type throws with both types named', () {
      final bytes = RaftWire.encodeDoc(1, (w) => w.integer('n', 5));
      final r = RaftWire.decodeDoc(
        bytes,
        ByteData.view(bytes.buffer),
        0,
        bytes.length,
      );
      expect(() => r.string('n'), throwsA(isA<StateError>()));
    });

    test('orNull getters return values when present, null when absent', () {
      final bytes = RaftWire.encodeDoc(1, (w) {
        w
          ..integer('n', 9)
          ..float('f', 1.5)
          ..boolean('b', false);
      });
      final r = RaftWire.decodeDoc(
        bytes,
        ByteData.view(bytes.buffer),
        0,
        bytes.length,
      );
      expect(r.integerOrNull('n'), 9);
      expect(r.floatOrNull('f'), 1.5);
      expect(r.booleanOrNull('b'), isFalse);
      expect(r.integerOrNull('missing'), isNull);
      expect(r.floatOrNull('missing'), isNull);
      expect(r.booleanOrNull('missing'), isNull);
      expect(r.has('missing'), isFalse);
    });

    test('truncated document is rejected', () {
      final bytes = RaftWire.encodeDoc(1, (w) => w.string('s', 'hello'));
      expect(
        () => RaftWire.decodeDoc(
          bytes,
          ByteData.view(bytes.buffer),
          0,
          bytes.length - 1,
        ),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
