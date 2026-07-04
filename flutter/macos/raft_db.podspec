#
# raft_db — macOS plugin podspec.
#
# Vendors the prebuilt universal (x86_64 + arm64) raft-db dynamic library
# so `flutter run -d macos` works with zero extra build steps. The Dart
# side loads symbols via DynamicLibrary.process(), which sees the dylib
# once CocoaPods links it into the app bundle.
#
Pod::Spec.new do |s|
  s.name             = 'raft_db'
  s.version          = '0.1.0'
  s.summary          = 'raft-db: offline-first embedded database for Flutter.'
  s.description      = <<-DESC
Mobile-native embedded database with durable-by-default writes, CRDT merge
semantics, and frame-loop-safe reads. This pod bundles the prebuilt macOS
universal binary of the Rust core.
                       DESC
  s.homepage         = 'https://github.com/AdebayoGit/raft'
  s.license          = { :type => 'Apache-2.0 OR MIT', :file => '../LICENSE' }
  s.author           = { 'Raft' => 'noreply@raftdb.dev' }

  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'
  s.vendored_libraries = 'Libs/libraftdb.dylib'

  s.dependency 'FlutterMacOS'
  s.platform = :osx, '10.14'
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES' }
  s.swift_version = '5.0'
end
