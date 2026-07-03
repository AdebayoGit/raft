# Thread affinity

Which thread calls you back, which thread you may call from, and how
each binding marshals events onto its platform's expected thread.

## The C-ABI threading contract

### Calling into Raft

- Every `rft_*` function is **thread-safe** and may be called from any
  thread. Handles (`RaftDb`, `RaftQueryResult`, `RaftTransaction`) are
  internally synchronized.
- Calls are **blocking**: they perform I/O on the calling thread. Do
  not call them on a platform main/UI thread — every binding wraps
  them in its platform's off-main-thread primitive (see below).

### Callbacks out of Raft

- **Mutation observers** (`rft_observe`): the callback fires on a
  **Rust runtime worker thread** — never the registering thread and
  never the platform main thread. Enforced by
  `observe_callbacks_fire_off_registering_thread` in
  `core/src/ffi/mod.rs`.
- **Live queries** (`rft_observe_query`): the **initial snapshot fires
  synchronously on the registering thread**, before the call returns.
  Every subsequent diff fires on a runtime worker thread. Enforced by
  `observe_query_snapshot_sync_then_diffs_off_thread`.
- **Dart-port observers** (`rft_observe_dart_port`,
  `rft_observe_query_dart_port`): events are posted with
  `Dart_PostCObject_DL` from a runtime worker thread; the Dart VM
  delivers them on the owning **isolate's event loop**, so Dart code
  never sees a foreign thread.

### Rules for callback implementations

1. **Do not block.** The callback runs inline on a runtime worker; a
   slow callback stalls event delivery for every subscription sharing
   that worker.
2. **Do not call back into Raft from inside a callback.** Blocking
   `rft_*` calls on a runtime worker thread can deadlock the runtime.
   Hand the event off to another thread first (all bindings do this).
3. **Copy the payload.** The JSON string is only valid for the
   duration of the call.

## Per-binding thread marshaling

| | Blocking calls run on | Native callback fires on | Your code receives events on |
|---|---|---|---|
| **Flutter** (`flutter/`) | the calling isolate (synchronous FFI) | n/a — Dart port, no C callback | the isolate event loop (`ReceivePort` → `Stream`) |
| **Kotlin** (`android/`) | `Dispatchers.IO` (suspending API) | tokio worker, attached to the JVM via `AttachCurrentThread` | the collector's coroutine context (`callbackFlow` + thread-safe `trySend`) |
| **Swift** (`swift/`) | `DispatchQueue.global(qos: .userInitiated)` behind `async` methods | tokio worker | the consuming task's actor — `AsyncStream.Continuation.yield` is thread-safe, so `for await` works from any actor, including `@MainActor` |
| **RN Nitro** (`rn/`) | the JS thread (Nitro sync/promise methods) | tokio worker | the JS thread — Nitro callbacks are thread-safe and dispatch onto the JS runtime via the CallInvoker |

Consequences per platform:

- **Flutter** — nothing to do; you are always on your isolate. Long
  queries block the calling isolate, so run heavy work in a separate
  isolate if needed.
- **Kotlin** — collect flows in whatever context you like;
  `flowOn`/`Dispatchers.Main` behave normally. Never call the
  suspending API from a thread you cannot block (it is safe: it always
  hops to `Dispatchers.IO`).
- **Swift** — consume `AsyncStream` from any actor. UI updates can
  `for await` directly inside a `@MainActor` task. Enforced by
  `ThreadAffinityTests.testObserveEventsAreDeliveredToAMainActorConsumer`
  in `swift/Tests/RaftDBTests`.
- **RN** — observer callbacks arrive on the JS thread; update state
  directly. Do not perform long synchronous work inside the callback
  or you block the JS runtime.

## Live-query initial snapshot

Because the initial `rft_observe_query` snapshot is synchronous, every
binding's `observeQuery` delivers the first `QueryDiff` **from the
registering call itself** (on the thread/context that made the call),
then switches to the async path for subsequent diffs. Bindings queue
that first diff through the same channel (Flow / AsyncStream / port /
callback) so consumers see a single, ordered stream.

## Enforcement

- Core: `observe_callbacks_fire_off_registering_thread` and
  `observe_query_snapshot_sync_then_diffs_off_thread`
  (`core/src/ffi/mod.rs`) pin the callback-thread contract.
- Swift: `ThreadAffinityTests` proves events produced on a tokio
  worker are consumable from the main actor.
- Kotlin/JNI: the trampoline in `android/src/main/cpp/raft-jni.cpp`
  attaches/detaches the runtime thread around every upcall;
  `callbackFlow` handles cross-thread emission.
- Flutter/RN: delivery relies on the Dart VM's port serialization and
  Nitro's CallInvoker dispatch respectively — both platform-guaranteed.
