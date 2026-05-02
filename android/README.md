# raftdb — Android (Kotlin)

> Mobile-native embedded database for Android. Idiomatic Kotlin over the Raft Rust core.

[![Maven Central](https://img.shields.io/maven-central/v/com.raftdb/raftdb.svg)](https://central.sonatype.com/artifact/com.raftdb/raftdb)

Offline-first storage with `suspend` operations, `Flow`-based observers, and a JNI bridge to the Raft core. No SQL, no schema migrations, no surprise battery drain.

## Install

`build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.raftdb:raftdb:0.1.0")
}
```

The artifact bundles `libraftdb.so` for `arm64-v8a`, `armeabi-v7a`, `x86_64`, and `x86`. ABI splits are honoured — your APK only ships the architectures you target.

## Quickstart

```kotlin
import com.raftdb.RaftDb
import com.raftdb.RaftCollection
import kotlinx.coroutines.flow.collect
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString

@Serializable
data class User(val id: String, val name: String, val age: Int)

class UserRepository(private val db: RaftDb) {

    private val users: RaftCollection<User> = RaftCollection(
        db = db,
        name = "users",
        serialize = { Json.encodeToString(it).toByteArray() },
        deserialize = { Json.decodeFromString(String(it)) },
    )

    suspend fun save(user: User) = users.put(user.id, user)

    suspend fun load(id: String): User? = users.get(id)

    suspend fun remove(id: String) = users.delete(id)
}

suspend fun example(context: Context) {
    val path = "${context.filesDir}/raftdb"
    val db = RaftDb.open(path)
    val repo = UserRepository(db)

    repo.save(User("1", "Alice", 30))
    val alice = repo.load("1")            // → User("1", "Alice", 30)
    repo.remove("1")

    db.close()
}
```

All blocking JNI calls are dispatched on `Dispatchers.IO` — call from any coroutine context.

## API

### `RaftDb.open(path)`

```kotlin
val db = RaftDb.open("/data/data/com.example/files/raft")
```

Suspends on `Dispatchers.IO`. There is also a `RaftDb.openBlocking(path)` for tests.

### `db.put(key, value)` / `db.get(key)` / `db.delete(key)`

Low-level byte operations. `put` and `delete` return `Unit`. `get` returns `ByteArray?`.

### `RaftCollection<T>`

Typed collection wrapper. Pass a serializer / deserializer pair (kotlinx.serialization, Moshi, Gson — your call). All keys are scoped under `<name>:<id>` so multiple collections share one database without collisions.

### `db.observe(prefix)` / `collection.observeAll()` → `Flow<QueryResult>`

A cold `Flow` that emits the current snapshot, then on every subsequent change matching the prefix.

```kotlin
viewModelScope.launch {
    db.observe("users:".toByteArray()).collect { result ->
        println("Key ${String(result.key)} changed")
    }
}
```

### `db.close()`

Releases the native handle. Idempotent — safe to call multiple times. Pair with `use { ... }` for autocloseable scoping in non-coroutine code.

## Errors

Native failures throw `RaftError` (a sealed class). Pattern-match on the subtype:

```kotlin
try {
    db.put(key, value)
} catch (e: RaftError.IoError) {
    Log.e("raft", "storage failure", e)
} catch (e: RaftError.NotFound) {
    // key missing
}
```

## Threading

- All native calls go through `Dispatchers.IO` — never call from `Dispatchers.Main`.
- The native handle is reference-counted internally; concurrent reads are safe.
- `close()` is atomic (`AtomicBoolean`) — calling it twice is harmless.

## Roadmap

The current Kotlin layer wraps the v0.1.0 KV surface. Native bridges for the document / query / transaction / observer FFI (already in `core/include/raft.h`) are the next milestone — they unlock first-class collections, predicate queries, and `Flow<T>` live queries.

## License

Apache-2.0 OR MIT.
