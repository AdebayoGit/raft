import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';

import 'raft_db_flutter_platform_interface.dart';

/// An implementation of [RaftDbFlutterPlatform] that uses method channels.
class MethodChannelRaftDbFlutter extends RaftDbFlutterPlatform {
  /// The method channel used to interact with the native platform.
  @visibleForTesting
  final methodChannel = const MethodChannel('raft_db_flutter');

  @override
  Future<String?> getPlatformVersion() async {
    final version = await methodChannel.invokeMethod<String>('getPlatformVersion');
    return version;
  }
}
