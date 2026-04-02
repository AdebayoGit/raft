import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';
import 'package:raft_db_flutter/raft_db_flutter.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatefulWidget {
  const MyApp({super.key});

  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  String _status = 'Initialising…';

  @override
  void initState() {
    super.initState();
    _runDemo();
  }

  Future<void> _runDemo() async {
    try {
      final dir = await getApplicationDocumentsDirectory();
      final dbPath = '${dir.path}${Platform.pathSeparator}raft_example';

      final db = await RaftDb.open(dbPath);

      // Write
      await db.put(
        utf8.encode('greeting'),
        utf8.encode('Hello from Raft!'),
      );

      // Read
      final value = await db.get(utf8.encode('greeting'));
      final decoded =
          value != null ? utf8.decode(value) : '<not found>';

      // Clean up
      await db.delete(utf8.encode('greeting'));
      await db.close();

      setState(() {
        _status = 'Running on: ${Platform.operatingSystem}\n'
            'Value: $decoded';
      });
    } catch (e) {
      setState(() {
        _status = 'Running on: ${Platform.operatingSystem}\nError: $e';
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('Raft DB Example')),
        body: Center(
          child: Text(_status),
        ),
      ),
    );
  }
}
