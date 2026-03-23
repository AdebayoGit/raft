import 'package:plugin_platform_interface/plugin_platform_interface.dart';

import 'raft_db_flutter_method_channel.dart';

abstract class RaftDbFlutterPlatform extends PlatformInterface {
  /// Constructs a RaftDbFlutterPlatform.
  RaftDbFlutterPlatform() : super(token: _token);

  static final Object _token = Object();

  static RaftDbFlutterPlatform _instance = MethodChannelRaftDbFlutter();

  /// The default instance of [RaftDbFlutterPlatform] to use.
  ///
  /// Defaults to [MethodChannelRaftDbFlutter].
  static RaftDbFlutterPlatform get instance => _instance;

  /// Platform-specific implementations should set this with their own
  /// platform-specific class that extends [RaftDbFlutterPlatform] when
  /// they register themselves.
  static set instance(RaftDbFlutterPlatform instance) {
    PlatformInterface.verifyToken(instance, _token);
    _instance = instance;
  }

  Future<String?> getPlatformVersion() {
    throw UnimplementedError('platformVersion() has not been implemented.');
  }
}
