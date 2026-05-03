// ---------------------------------------------------------------------------
// Mock store — shared state between mock and tests
// ---------------------------------------------------------------------------

const mockStore = new Map<string, string>()
let mockIsOpen = false

// Track calls for assertions
const mockCalls = {
  open: jest.fn(),
  put: jest.fn(),
  get: jest.fn(),
  delete: jest.fn(),
  close: jest.fn(),
  watch: jest.fn(),
  unwatch: jest.fn(),
}

// ---------------------------------------------------------------------------
// Mock the NitroModules native layer (hoisted before imports)
// ---------------------------------------------------------------------------

jest.mock('react-native-nitro-modules', () => ({
  NitroModules: {
    createHybridObject: jest.fn(() => ({
      open: (path: string) => {
        mockCalls.open(path)
        if (mockIsOpen) throw new Error('Already open')
        mockIsOpen = true
      },
      put: async (key: string, value: string) => {
        mockCalls.put(key, value)
        if (!mockIsOpen) throw new Error('Not open')
        mockStore.set(key, value)
      },
      get: async (key: string) => {
        mockCalls.get(key)
        if (!mockIsOpen) throw new Error('Not open')
        return mockStore.get(key) ?? undefined
      },
      delete: async (key: string) => {
        mockCalls.delete(key)
        if (!mockIsOpen) throw new Error('Not open')
        const prev = mockStore.get(key) ?? undefined
        mockStore.delete(key)
        return prev
      },
      close: () => {
        mockCalls.close()
        mockIsOpen = false
        mockStore.clear()
      },
      watch: (
        query: string,
        callback: (result: { key: string; value: string | undefined }) => void
      ) => {
        mockCalls.watch(query, callback)
        const current = mockStore.get(query)
        callback({ key: query, value: current })
        return 'sub-123'
      },
      unwatch: (subscriptionId: string) => {
        mockCalls.unwatch(subscriptionId)
      },
    })),
  },
}))

// Import after mock is set up
import { RaftDB } from '../index'

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('RaftDB', () => {
  beforeEach(() => {
    Object.values(mockCalls).forEach((fn) => fn.mockClear())
    mockStore.clear()
    mockIsOpen = false
  })

  // -- open ----------------------------------------------------------------

  describe('open', () => {
    it('opens a database and returns a RaftDB instance', () => {
      const db = RaftDB.open('/tmp/test.db')
      expect(db).toBeInstanceOf(RaftDB)
      expect(db.isClosed).toBe(false)
    })

    it('calls native open with the provided path', () => {
      RaftDB.open('/data/mydb')
      expect(mockCalls.open).toHaveBeenCalledWith('/data/mydb')
    })
  })

  // -- put -----------------------------------------------------------------

  describe('put', () => {
    it('inserts a key-value pair', async () => {
      const db = RaftDB.open('/tmp/test.db')
      await db.put('greeting', 'hello')
      expect(mockCalls.put).toHaveBeenCalledWith('greeting', 'hello')
      expect(mockStore.get('greeting')).toBe('hello')
    })

    it('overwrites an existing key', async () => {
      const db = RaftDB.open('/tmp/test.db')
      await db.put('key', 'v1')
      await db.put('key', 'v2')
      expect(mockStore.get('key')).toBe('v2')
    })

    it('throws when database is closed', async () => {
      const db = RaftDB.open('/tmp/test.db')
      db.close()
      await expect(db.put('k', 'v')).rejects.toThrow('closed')
    })
  })

  // -- get -----------------------------------------------------------------

  describe('get', () => {
    it('returns the value for an existing key', async () => {
      const db = RaftDB.open('/tmp/test.db')
      await db.put('key', 'value')
      const result = await db.get('key')
      expect(result).toBe('value')
    })

    it('returns null for a missing key', async () => {
      const db = RaftDB.open('/tmp/test.db')
      const result = await db.get('nonexistent')
      expect(result).toBeNull()
    })

    it('throws when database is closed', async () => {
      const db = RaftDB.open('/tmp/test.db')
      db.close()
      await expect(db.get('k')).rejects.toThrow('closed')
    })
  })

  // -- delete --------------------------------------------------------------

  describe('delete', () => {
    it('removes a key and returns the previous value', async () => {
      const db = RaftDB.open('/tmp/test.db')
      await db.put('key', 'value')
      const prev = await db.delete('key')
      expect(prev).toBe('value')
      expect(mockStore.has('key')).toBe(false)
    })

    it('returns null when deleting a non-existent key', async () => {
      const db = RaftDB.open('/tmp/test.db')
      const prev = await db.delete('ghost')
      expect(prev).toBeNull()
    })

    it('throws when database is closed', async () => {
      const db = RaftDB.open('/tmp/test.db')
      db.close()
      await expect(db.delete('k')).rejects.toThrow('closed')
    })
  })

  // -- close ---------------------------------------------------------------

  describe('close', () => {
    it('marks the database as closed', () => {
      const db = RaftDB.open('/tmp/test.db')
      expect(db.isClosed).toBe(false)
      db.close()
      expect(db.isClosed).toBe(true)
    })

    it('is idempotent — multiple close calls do not throw', () => {
      const db = RaftDB.open('/tmp/test.db')
      db.close()
      db.close()
      db.close()
      expect(mockCalls.close).toHaveBeenCalledTimes(1)
    })
  })

  // -- watch / unwatch -----------------------------------------------------

  describe('watch', () => {
    it('registers an observer and receives initial snapshot', () => {
      const db = RaftDB.open('/tmp/test.db')
      const results: Array<{ key: string; value: string | undefined }> = []
      db.watch('prefix', (result) => results.push(result))

      expect(mockCalls.watch).toHaveBeenCalled()
      expect(results).toHaveLength(1)
      expect(results[0]!.key).toBe('prefix')
    })

    it('returns an unsubscribe function', () => {
      const db = RaftDB.open('/tmp/test.db')
      const unsub = db.watch('key', () => {})
      expect(typeof unsub).toBe('function')

      unsub()
      expect(mockCalls.unwatch).toHaveBeenCalledWith('sub-123')
    })

    it('throws when database is closed', () => {
      const db = RaftDB.open('/tmp/test.db')
      db.close()
      expect(() => db.watch('k', () => {})).toThrow('closed')
    })
  })

  // -- integration-style sequence ------------------------------------------

  describe('full lifecycle', () => {
    it('open → put → get → delete → close', async () => {
      const db = RaftDB.open('/tmp/lifecycle.db')

      await db.put('user:1', '{"name":"Alice"}')
      const val = await db.get('user:1')
      expect(val).toBe('{"name":"Alice"}')

      const deleted = await db.delete('user:1')
      expect(deleted).toBe('{"name":"Alice"}')

      const gone = await db.get('user:1')
      expect(gone).toBeNull()

      db.close()
      expect(db.isClosed).toBe(true)
    })
  })

  // -- edge cases ----------------------------------------------------------

  describe('edge cases', () => {
    it('JSON payloads round-trip without mutation', async () => {
      const db = RaftDB.open('/tmp/json.db')

      const original = {
        id: '1',
        name: 'Alice',
        tags: ['admin', 'beta'],
        meta: { joined: '2024-01-01', flags: { newUi: true } },
      }
      await db.put('user:1', JSON.stringify(original))
      const restored = JSON.parse((await db.get('user:1'))!)

      expect(restored).toEqual(original)
    })

    it('empty-string values are stored and retrieved as ""', async () => {
      const db = RaftDB.open('/tmp/empty.db')
      await db.put('flag', '')
      const val = await db.get('flag')
      expect(val).toBe('')
    })

    it('overlapping prefixes do not collide on get', async () => {
      const db = RaftDB.open('/tmp/prefix.db')
      await db.put('user', 'plain')
      await db.put('user:1', 'scoped')

      expect(await db.get('user')).toBe('plain')
      expect(await db.get('user:1')).toBe('scoped')
    })

    it('non-ascii keys and values round-trip', async () => {
      const db = RaftDB.open('/tmp/utf8.db')
      const key = 'ユーザー🚀café'
      const value = '日本語 — éàü 🌍'
      await db.put(key, value)
      expect(await db.get(key)).toBe(value)
    })

    it('many concurrent puts settle without dropping writes', async () => {
      const db = RaftDB.open('/tmp/concurrent.db')
      const writes = Array.from({ length: 50 }, (_, i) =>
        db.put(`k${i}`, `v${i}`)
      )
      await Promise.all(writes)

      // Spot-check a few keys.
      expect(await db.get('k0')).toBe('v0')
      expect(await db.get('k25')).toBe('v25')
      expect(await db.get('k49')).toBe('v49')
      expect(mockCalls.put).toHaveBeenCalledTimes(50)
    })
  })

  // -- close / re-open behaviour -------------------------------------------

  describe('close behaviour', () => {
    it('post-close get returns rejected promise (not silent null)', async () => {
      const db = RaftDB.open('/tmp/post-close.db')
      await db.put('k', 'v')
      db.close()

      await expect(db.get('k')).rejects.toThrow('closed')
    })

    it('post-close delete throws', async () => {
      const db = RaftDB.open('/tmp/post-close-del.db')
      db.close()
      await expect(db.delete('k')).rejects.toThrow('closed')
    })

    it('isClosed reflects state across instances independently', () => {
      const a = RaftDB.open('/tmp/a.db')
      const b = RaftDB.open('/tmp/b.db')
      a.close()
      expect(a.isClosed).toBe(true)
      expect(b.isClosed).toBe(false)
      b.close()
      expect(b.isClosed).toBe(true)
    })
  })

  // -- watch edge cases ----------------------------------------------------

  describe('watch edge cases', () => {
    it('multiple watchers can subscribe to the same prefix', () => {
      const db = RaftDB.open('/tmp/multi-watch.db')
      const a: string[] = []
      const b: string[] = []
      db.watch('users:', (r) => a.push(r.key))
      db.watch('users:', (r) => b.push(r.key))

      // Each watcher receives its own initial snapshot.
      expect(a).toHaveLength(1)
      expect(b).toHaveLength(1)
      expect(mockCalls.watch).toHaveBeenCalledTimes(2)
    })

    it('unsubscribe is callable after close without throwing', () => {
      const db = RaftDB.open('/tmp/unsub-after-close.db')
      const unsub = db.watch('k', () => {})
      db.close()

      // After close, the unsubscribe handle is now stale. Closing did
      // call native unwatch implicitly via the mock — calling the
      // returned function should still be a no-op rather than crash.
      expect(() => unsub()).not.toThrow()
    })
  })
})
