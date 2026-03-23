import 'package:flutter_test/flutter_test.dart';
import 'package:raft_db_flutter/raft_db_flutter.dart';

void main() {
  // RaftDb.open requires a real compiled native library, so runtime integration
  // tests live in integration_test/. These unit tests cover the pure-Dart
  // surface: exception formatting and closed-state guards.

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
}
