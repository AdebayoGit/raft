#!/bin/bash
set -e

cd core

echo "==> Building Android..."
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 \
  -o ../flutter/android/src/main/jniLibs \
  build --release --features ffi

echo "==> Building iOS..."
cargo build --release --target aarch64-apple-ios --features ffi
cargo build --release --target aarch64-apple-ios-sim --features ffi
cargo build --release --target x86_64-apple-ios --features ffi

lipo -create \
  target/aarch64-apple-ios-sim/release/libraftdb.a \
  target/x86_64-apple-ios/release/libraftdb.a \
  -output target/libraftdb-sim.a

xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libraftdb.a \
  -library target/libraftdb-sim.a \
  -output ../flutter/ios/RaftDB.xcframework

echo "==> Done."