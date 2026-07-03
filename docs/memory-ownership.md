# FFI memory ownership

Who allocates, who frees, and how each binding upholds the contract. The
canonical rules live in the `core/src/ffi/mod.rs` module docs and in
`core/include/raft.h`; this page is the cross-platform reference.

## The C-ABI ownership contract

### Database handle (`RaftDb`)

- `rft_open` / `rft_open_at` allocate the handle; **the caller owns it**.
- `rft_close` frees it. Closing also aborts every pending observer task,
  so no callback fires after `rft_close` returns.
- Every other function borrows the handle for the duration of the call.
  Using a handle after `rft_close` is undefined behaviour — bindings
  guard against this with an `isClosed` / `closed` flag.

### Query result handle (`RaftQueryResult`)

- `rft_query_execute` allocates the result; **the caller owns it** and
  must call `rft_query_result_free` exactly once.
- `rft_query_result_count` / `rft_query_result_get` borrow the handle.
- Double-free protection: results are tracked in a live-handle registry;
  the free path unregisters first ("unregister wins"), so a second
  `rft_query_result_free` on the same pointer is a safe no-op error
  (`InvalidHandle`), not a crash.

### Transaction handle (`RaftTransaction`)

- `rft_transaction_begin` allocates; **the caller owns it**.
- The handle is **consumed** by `rft_transaction_commit` *or*
  `rft_transaction_rollback` — call exactly one of them, once. The
  registry gives the same unregister-wins double-free protection.
- Forgetting to call either leaks the transaction (its read/write sets)
  until process exit. Bindings prevent this with scoped
  `withTransaction`-style APIs and destructor-based rollback.

### Returned bytes: the buffer-too-small protocol

Functions that return variable-length bytes (`rft_get`,
`rft_collection_get`, `rft_transaction_get`, `rft_query_result_get`)
never allocate for the caller. The caller passes `out_buf` +
`*out_len`:

1. If the buffer is large enough, bytes are copied and `*out_len` is set
   to the actual length.
2. If not, the call returns `BufferTooSmall`, `*out_len` holds the
   required size, and **no bytes are copied**. Call again with a bigger
   buffer.

The core never hands out pointers into its own storage, so there is
nothing to free and no lifetime coupling.

### Input slices

Key/value/JSON byte slices and C strings passed *into* the FFI are
**borrowed only for the duration of the call**. The core copies what it
needs before returning; callers may free or reuse the memory
immediately after.

### Observer subscriptions

- `rft_observe` / `rft_observe_query` register a C callback plus a
  `user_data` pointer. The callback and `user_data` must stay valid
  until `rft_unobserve(id)` returns or the database is closed —
  whichever comes first. `rft_close` aborts all observer tasks.
- Dart-port observers (`rft_observe_dart_port`,
  `rft_observe_query_dart_port`) post messages via
  `Dart_PostCObject_DL`, which serializes into the VM — the C strings
  only need to outlive the post call itself. **Caveat:** merely closing
  the Dart `ReceivePort` does not stop the background task; the
  subscription (and its task) lives until `rft_unobserve` or
  `rft_close`.

## Who frees what, per binding

| Resource | Flutter (`flutter/`) | Kotlin/JNI (`android/`) | Swift (`swift/`) | RN Nitro (`rn/`) |
|---|---|---|---|---|
| `RaftDb` | explicit `close()` → `rft_close` | explicit `close()` (AutoCloseable) → JNI `nativeClose` → `rft_close` | explicit `close()` + `deinit` fallback → `rft_close` | iOS: `close()` + `deinit`; Android: `close()` + `finalize()` — both unobserve typed observers first, then `rft_close` |
| `RaftQueryResult` | freed in `query()` after copying rows (`rft_query_result_free`) | JNI shim frees on **all** paths, including partial-copy errors | `defer { rft_query_result_free(handle) }` | iOS: `defer` free; Android: JNI shim frees on all paths |
| `RaftTransaction` | `withTransaction` commits or rolls back; explicit `rollback()` otherwise | `withTransaction`-style scope; JNI rollback on error paths | `withTransaction` + `deinit` rolls back an unfinalised transaction | `RaftDB.withTransaction` commits/rolls back; `RaftTransaction` tracks `consumed` |
| Out buffers | Dart-side `malloc`/`free` around the two-call protocol | JNI shim allocates Java byte arrays; native buffer freed in shim | Swift `[UInt8]` stack/heap arrays — ARC managed | JNI/Swift shims own the temporary buffers |
| Observer context | Dart port — nothing to pin; unsubscribe via `rft_unobserve` | pinned callback ref + context address held until `unobserve`/`close` | callback context retained by the subscription wrapper until `unobserve`/`close` | `JsCallbackContext` retained per subscription; released on `unwatch`/`close` |

Rules every binding follows:

1. **One free per handle.** The unregister-wins registry makes a
   double free an error return, not memory corruption — but bindings
   still guarantee exactly-once frees structurally (defer/deinit/
   finally).
2. **Query results are freed on every path**, including early returns
   on row-copy errors.
3. **Transactions are never abandoned.** Scoped APIs commit on success
   and roll back on error; Swift additionally rolls back in `deinit` as
   a last resort.
4. **`close()` is idempotent** at the binding layer (a `closed` flag)
   even though `rft_close` itself must only be called once per handle.

## Leak verification

CI runs the FFI test suite under AddressSanitizer with leak detection
(the `leak-sanitizer` job in `.github/workflows/ci.yml`). Since every
binding drives the same C ABI, an LSAN-clean FFI suite covers the
native side of all four bindings.

Run it locally (nightly toolchain required):

```sh
# Linux
RUSTFLAGS="-Zsanitizer=address" ASAN_OPTIONS="detect_leaks=1" \
  cargo +nightly test --features ffi --lib --target x86_64-unknown-linux-gnu

# macOS (Apple Silicon) — LeakSanitizer is not supported on darwin;
# ASan still catches use-after-free / double-free
RUSTFLAGS="-Zsanitizer=address" \
  cargo +nightly test --features ffi --lib --target aarch64-apple-darwin
```
