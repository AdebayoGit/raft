#!/bin/bash
set -e

cd core

echo "==> Building Android..."
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 \
  -o ../flutter/android/src/main/jniLibs \
  build --release --features ffi

echo "==> Copying Android libs to all platforms..."
cp -r ../flutter/android/src/main/jniLibs/* ../android/src/main/jniLibs/
cp -r ../flutter/android/src/main/jniLibs/* ../rn/android/src/main/jniLibs/

echo "==> Building iOS..."
cargo build --release --target aarch64-apple-ios --features ffi
cargo build --release --target aarch64-apple-ios-sim --features ffi
cargo build --release --target x86_64-apple-ios --features ffi

echo "==> Creating fat simulator binary..."
lipo -create \
  target/aarch64-apple-ios-sim/release/libraftdb.a \
  target/x86_64-apple-ios/release/libraftdb.a \
  -output target/libraftdb-sim.a

echo "==> Packaging XCFramework..."
rm -rf ../flutter/ios/RaftDB.xcframework

xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libraftdb.a \
  -library target/libraftdb-sim.a \
  -output ../flutter/ios/RaftDB.xcframework

echo "==> Copying XCFramework to all platforms..."
rm -rf ../swift/RaftDB.xcframework
rm -rf ../rn/ios/RaftDB.xcframework

cp -r ../flutter/ios/RaftDB.xcframework ../swift/
cp -r ../flutter/ios/RaftDB.xcframework ../rn/ios/

echo "==> Generating C header..."
cbindgen --config cbindgen.toml --crate raft-db --output include/raft.h

echo "==> Build complete. Libraries distributed to:"
echo "    flutter/android/src/main/jniLibs/"
echo "    flutter/ios/RaftDB.xcframework"
echo "    android/src/main/jniLibs/"
echo "    swift/RaftDB.xcframework"
echo "    rn/android/src/main/jniLibs/"
echo "    rn/ios/RaftDB.xcframework"
echo "    core/include/raft.h"