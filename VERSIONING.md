# Versioning Policy

Raft follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html). The same `MAJOR.MINOR.PATCH` version is used across every published artifact:

| Registry        | Package              |
|-----------------|----------------------|
| crates.io       | `raft-db`            |
| pub.dev         | `raft_db`            |
| Maven Central   | `com.raftdb:raftdb`  |
| Swift Pkg Index | `RaftDB`             |
| npm             | `react-native-raft`  |

A release of v0.X.Y publishes the same version on every channel — no platform is allowed to drift.

## What counts as a breaking change

Any of the following triggers a **MAJOR** bump:

- Removed or renamed function in `core/include/raft.h` (the C ABI is the contract every platform binding depends on).
- Removed / renamed public Rust item in `raft-db` crate.
- Removed / renamed public type or method in any platform binding.
- Wire format change in JSON envelopes (`Document`, `Filter`, `QueryDiff`, `MutationEvent`) that breaks round-tripping with an older platform binding.
- Storage format change that prevents an older binary from opening a database written by a newer one.
- Sync protocol change that breaks compatibility with an existing reference server.

## What counts as a feature

Any of the following triggers a **MINOR** bump:

- New `rft_*` C function added.
- New public Rust item or platform-binding method.
- New optional field in a JSON envelope (with serde default).
- New CRDT type, query predicate, or sync mode.

## What counts as a patch

Any of the following triggers a **PATCH** bump:

- Bug fix that doesn't change the public API or wire format.
- Performance improvement.
- Documentation update.
- Internal refactor.

## Pre-1.0

While the major version is `0`, **MINOR** bumps may include breaking changes. We document breaking changes prominently in `CHANGELOG.md` regardless. Once 1.0.0 ships, the rules above are strict.

## Release process

1. Update `version` in `core/Cargo.toml`.
2. Update version in each platform package manifest:
   - `flutter/pubspec.yaml`
   - `android/build.gradle.kts` (`version =` block)
   - `swift/Package.swift` (no version field — released via Git tags)
   - `rn/package.json`
3. Update `CHANGELOG.md` — move `[Unreleased]` items under the new version with the date.
4. Commit, tag (`git tag v0.X.Y`), and push.
5. Publish in this order so each registry can find the previous step's artifacts:
   1. `cargo publish` (crates.io)
   2. Build mobile artifacts via `./build-mobile.sh`
   3. `flutter pub publish`
   4. Gradle publish (Maven Central)
   5. Git tag is what Swift Package Index picks up
   6. `bun publish` (npm)
6. Create a GitHub release pointing at the tag with binary attachments for each platform.

## Compatibility windows

- The C ABI guarantees compatibility within a major version. A `0.2.x` platform binding works against a `0.2.y` core.
- Storage format guarantees compatibility within a major version. A `0.2.x` database opens cleanly with a `0.2.y` binary.
- Sync protocol guarantees compatibility within a major version (once sync ships in v0.2.0).
