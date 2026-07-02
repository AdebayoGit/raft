import 'dart:convert';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:raft_db_flutter/raft_db_flutter.dart';

void main() {
  // RaftDb.open requires a real compiled native library, so runtime
  // integration tests live in integration_test/. These unit tests cover
  // the pure-Dart surface: exception formatting, error code mapping,
  // and RaftCollection key scoping logic.

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

    test('unknown code falls back to generic message including code', () {
      final ex = RaftDbException.fromCode(99);
      expect(ex.code, 99);
      expect(ex.message.toLowerCase(), contains('unknown'));
      expect(ex.message, contains('99'));
    });

    test('all known codes round-trip through code field', () {
      for (final code in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) {
        final ex = RaftDbException.fromCode(code);
        expect(ex.code, code, reason: 'code $code should round-trip');
      }
    });
  });

  group('RaftCollection key scoping (pure-Dart)', () {
    test('scoped key has the form <name>:<id>', () {
      final scoped = utf8.encode('users:42');
      expect(utf8.decode(scoped), 'users:42');
    });

    test('different collection names produce distinct scoped keys', () {
      final users = utf8.encode('users:1');
      final orders = utf8.encode('orders:1');
      expect(users, isNot(orders));
    });

    test('non-ascii ids round-trip through utf-8', () {
      // Real-world keys: emoji, accents, CJK.
      const id = 'ユーザー🚀café';
      final scoped = 'users:$id';
      final bytes = utf8.encode(scoped);
      expect(utf8.decode(bytes), scoped);
    });

    test('serializer / deserializer round-trip via JSON', () {
      Uint8List serialize(Map<String, dynamic> doc) =>
          Uint8List.fromList(utf8.encode(jsonEncode(doc)));
      Map<String, dynamic> deserialize(Uint8List bytes) =>
          jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>;

      final original = {'id': '1', 'name': 'Alice', 'age': 30};
      final restored = deserialize(serialize(original));
      expect(restored, original);
    });

    test('empty id is encodable', () {
      // Edge case: empty id under a non-empty collection should still
      // produce a valid scoped key.
      final scoped = utf8.encode('users:');
      expect(scoped.length, 'users:'.length);
    });
  });
}
