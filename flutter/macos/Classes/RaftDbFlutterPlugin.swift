import Cocoa
import FlutterMacOS

/// Minimal registrant: raft_db_flutter is an FFI plugin — all database
/// calls go straight to the vendored Rust dylib via dart:ffi. This class
/// exists so CocoaPods links the library into the app bundle and to answer
/// the plugin handshake.
public class RaftDbFlutterPlugin: NSObject, FlutterPlugin {
  public static func register(with registrar: FlutterPluginRegistrar) {
    let channel = FlutterMethodChannel(
      name: "raft_db_flutter", binaryMessenger: registrar.messenger)
    let instance = RaftDbFlutterPlugin()
    registrar.addMethodCallDelegate(instance, channel: channel)
  }

  public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
    switch call.method {
    case "getPlatformVersion":
      result("macOS " + ProcessInfo.processInfo.operatingSystemVersionString)
    default:
      result(FlutterMethodNotImplemented)
    }
  }
}
