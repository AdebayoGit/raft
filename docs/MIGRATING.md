# Migrating to raft-db

Both Isar (unmaintained, community fork only) and Hive (caretaker mode) have
left Flutter teams looking for a durable home. This guide maps their concepts
to raft's.

## From Isar

| Isar | raft-db |
|---|---|
| `@collection class Todo { Id id; ... }` + build_runner | plain class + two closures — **no codegen** |
| `isar.todos.put(t)` (async, inside `writeTxn`) | `todos.put(t)` — sync, durable before it returns |
| `isar.writeTxn(() => todos.putAll(list))` | `todos.putAll(list)` — one atomic commit |
| `isar.todos.get(id)` (async) | `todos.get(id)` — sync, ~1 µs |
| `isar.todos.getAll(ids)` | `todos.getMany(ids)` — one FFI crossing |
| `where().findAll()` | `todos.all()` or predicate queries (`db.executeQuery`) |
| `watchLazy()` | `todos.watch()` |
| — (no equivalent) | `todos.getCached(id)` — zero-FFI hot reads, safe in `build()` |

Key differences to plan around:
- **Ids are `int`** and supplied by your model (`id: (t) => t.id`), or use
  the auto-assign path (`db.collectionPutAuto`).
- **Writes are durable per commit.** Isar's writes are transaction-durable
  too, but raft is the only engine whose flush is verified down to
  `F_FULLFSYNC`. Batch with `putAll` — one fsync for the whole batch.
- **Writers scale.** Isar holds a global writer lock (concurrent writers
  gain nothing); raft's group commit merges concurrent fsyncs (~2× at 4
  writers).

## From Hive

| Hive | raft-db |
|---|---|
| `Hive.openBox<T>('todos')` + `TypeAdapter` registration | `db.collection<Todo>(...)` — closures, no adapters |
| `box.put(key, value)` | `todos.put(t)` |
| `box.get(key)` | `todos.get(id)` / `todos.getCached(id)` |
| `box.values` | `todos.all()` |
| `box.watch()` | `todos.watch()` |

The honest difference: **Hive never fsyncs**, which is where its speed comes
from — a crash can lose acknowledged writes. raft's writes are durable when
the call returns, and its cached reads (`getCached`) still give you
RAM-speed hot reads (measured 4.8× Hive's reads on the same machine) without
giving up crash safety.

## The mental-model shift

raft is offline-first with a merge surface: every field is backed by a CRDT
and stamped with a hybrid logical clock, so when you later add sync (your
backend or Relay), concurrent edits from two devices converge
deterministically — no conflict dialogs, no "last write wins" surprises you
didn't choose. You don't have to think about any of this until the day you
need it; the schema defaults to pure local semantics.

## Rust counter checked values

`Counter::value()` and `Counter::device_delta()` now return `Result<i64, CounterOverflow>` instead of silently wrapping values outside the signed 64-bit range. Handle the error or use `exact_value()` / `exact_device_delta()` when an `i128` result is appropriate. The serialized PN-counter representation and merge behavior are unchanged.
