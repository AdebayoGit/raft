# react-native-raft

> Mobile-native embedded database for React Native, built with Nitro Modules over the Raft Rust core.

[![Version](https://img.shields.io/npm/v/react-native-raft.svg)](https://www.npmjs.com/package/react-native-raft)
[![Downloads](https://img.shields.io/npm/dm/react-native-raft.svg)](https://www.npmjs.com/package/react-native-raft)
[![License](https://img.shields.io/npm/l/react-native-raft.svg)](LICENSE)

Offline-first storage for RN apps with synchronous-feeling Promise APIs, live observers via callbacks, and JSI bindings (no bridge round-trips).

## Requirements

- React Native v0.76.0 or higher (v0.78.0+ recommended for Nitro Views)
- Node 18.0.0 or higher

## Install

```bash
bun add react-native-raft react-native-nitro-modules
# or: npm install / yarn add
```

iOS:

```bash
cd ios && pod install
```

Android: no extra steps. The native library is autolinked.

## Quickstart

```ts
import { RaftDB, type QueryResult } from 'react-native-raft'
import RNFS from 'react-native-fs'

async function example() {
  const path = `${RNFS.DocumentDirectoryPath}/raft.db`
  const db = RaftDB.open(path)

  // Writes
  await db.put('user:1', JSON.stringify({ name: 'Alice', age: 30 }))

  // Reads — null if missing
  const raw = await db.get('user:1')
  if (raw) {
    const user = JSON.parse(raw)
    console.log('Loaded:', user)
  }

  // Live observation — fires every time a matching key is written / deleted
  const unsubscribe = db.watch('user:', (diff: QueryResult) => {
    console.log('Change:', diff)
  })

  // Later
  unsubscribe()

  // Deletes return the previous value, or null
  await db.delete('user:1')

  db.close()
}
```

`RaftDB.open` is synchronous (the underlying open is fast). All read/write methods are `Promise`-returning so they don't block the JS thread.

## API

### `RaftDB.open(path: string): RaftDB`

Open or create a database. Throws if the path is invalid or the native library failed to load.

### `db.put(key: string, value: string): Promise<void>`

Insert or update a key. Both arguments are strings — encode JSON / base64 yourself if you need richer types.

### `db.get(key: string): Promise<string | null>`

Returns the value, or `null` if the key does not exist.

### `db.delete(key: string): Promise<string | null>`

Returns the previous value (if any), or `null`. Always succeeds (deletion of a missing key is a no-op).

### `db.watch(prefix: string, callback): () => void`

Register a live-query observer for keys matching `prefix`. The callback receives a `QueryResult` (key + current value, or `null` on delete). Returns an unsubscribe function.

```ts
const unsubscribe = db.watch('users:', ({ key, value }) => {
  if (value === null) {
    console.log(`${key} was deleted`)
  } else {
    console.log(`${key} → ${value}`)
  }
})
```

### `db.close(): void`

Releases the native handle. Safe to call multiple times. After `close()`, every method throws `Error('RaftDB is already closed')`.

### `db.isClosed: boolean`

Inspect close state without throwing.

## React hook example

```tsx
import { useEffect, useState } from 'react'
import { RaftDB } from 'react-native-raft'

function useRaftValue(db: RaftDB, key: string) {
  const [value, setValue] = useState<string | null>(null)

  useEffect(() => {
    db.get(key).then(setValue)
    return db.watch(key, ({ value }) => setValue(value))
  }, [db, key])

  return value
}
```

## Architecture

Built on [Nitro Modules](https://github.com/mrousavy/nitro). The `Raft` hybrid object is generated from the `.nitro.ts` spec at `src/specs/raft.nitro.ts` and bridges directly to the C ABI exported by the Raft Rust core (`libraftdb.so` on Android, static `xcframework` on iOS).

No JSON serialisation between JS and native, no bridge queue — all calls hit the C layer directly.

## Roadmap

The current TypeScript layer wraps the v0.1.0 KV + watch surface. Bridging the document / query / transaction FFI (`core/include/raft.h`) is the next milestone — first-class typed collections, predicate queries, and optimistic transactions from JS.

## Credits

Bootstrapped with [create-nitro-module](https://github.com/patrickkabwe/create-nitro-module).

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss.

## License

Apache-2.0 OR MIT.
