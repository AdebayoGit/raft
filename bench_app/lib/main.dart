import 'package:flutter/material.dart';

import 'ui/home_page.dart';

void main() {
  runApp(const RaftBenchApp());
}

/// Root of the Raft cross-database benchmark app.
class RaftBenchApp extends StatelessWidget {
  const RaftBenchApp({super.key});

  static const _accent = Color(0xFFFF5A3C);

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Raft Benchmarks',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        useMaterial3: true,
        colorScheme: ColorScheme.fromSeed(
          seedColor: _accent,
          brightness: Brightness.light,
        ),
      ),
      darkTheme: ThemeData(
        useMaterial3: true,
        colorScheme: ColorScheme.fromSeed(
          seedColor: _accent,
          brightness: Brightness.dark,
        ),
      ),
      home: const HomePage(),
    );
  }
}
