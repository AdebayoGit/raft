// JNI shim: bridges Kotlin `external fun native*` declarations on
// `com.raft.HybridRaft` to the C ABI exported by `libraftdb.so`.
//
// The Kotlin layer in `HybridRaft.kt` uses standard JNI naming
// (`Java_<package>_<class>_<method>`) for its native methods. The Rust
// core only exports flat `rft_*` C symbols, so without this shim the
// runtime would throw `UnsatisfiedLinkError`.
//
// This file lives inside `libRaft.so` (the Nitro module library) which
// already links against the prebuilt `libraftdb.so`, so we can call
// `rft_*` directly here.

#include <jni.h>
#include <stdint.h>
#include <string.h>
#include <vector>

// ── C ABI declarations (mirroring core/include/raft.h) ────────────────

extern "C" {

typedef struct RaftDb RaftDb;

RaftDb* rft_open(const char* path, uint32_t* out_err);
void rft_close(RaftDb* db);
uint32_t rft_put(RaftDb* db, const uint8_t* key, size_t key_len,
                 const uint8_t* value, size_t value_len);
uint32_t rft_get(RaftDb* db, const uint8_t* key, size_t key_len,
                 uint8_t* out_value, size_t* out_len);
uint32_t rft_delete(RaftDb* db, const uint8_t* key, size_t key_len);

} // extern "C"

// Error codes that need special handling here. The full enum lives in
// `RftError` on the Kotlin side; we only branch on the ones that affect
// JNI buffer handling.
constexpr uint32_t RFT_ERROR_OK = 0;
constexpr uint32_t RFT_ERROR_NOT_FOUND = 4;
constexpr uint32_t RFT_ERROR_BUFFER_TOO_SMALL = 5;

namespace {

// Wrap a `*mut RaftDb` as `jlong` for ferrying through Kotlin. The
// Kotlin layer treats `0L` as "no handle".
jlong toHandle(RaftDb* db) {
    return reinterpret_cast<jlong>(db);
}

RaftDb* fromHandle(jlong handle) {
    return reinterpret_cast<RaftDb*>(handle);
}

// Read a Java `ByteArray` into a `std::vector<uint8_t>`. We materialise
// because `GetByteArrayElements` may pin or copy depending on the JVM —
// either way we need a contiguous buffer for the C ABI.
std::vector<uint8_t> readByteArray(JNIEnv* env, jbyteArray arr, jint len) {
    std::vector<uint8_t> buf(static_cast<size_t>(len));
    if (len > 0) {
        env->GetByteArrayRegion(arr, 0, len,
                                reinterpret_cast<jbyte*>(buf.data()));
    }
    return buf;
}

} // namespace

// ── JNI exports ───────────────────────────────────────────────────────
//
// Method names follow JNI mangling: `Java_<package>_<class>_<method>`,
// with `.` replaced by `_`. All `native` methods on
// `com.raft.HybridRaft` map here.

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_raft_HybridRaft_nativeOpen(JNIEnv* env, jclass /*clazz*/,
                                    jstring path) {
    const char* cPath = env->GetStringUTFChars(path, nullptr);
    if (cPath == nullptr) {
        return 0L;
    }
    uint32_t err = 0;
    RaftDb* db = rft_open(cPath, &err);
    env->ReleaseStringUTFChars(path, cPath);
    return (err == RFT_ERROR_OK) ? toHandle(db) : 0L;
}

JNIEXPORT void JNICALL
Java_com_raft_HybridRaft_nativeClose(JNIEnv* /*env*/, jclass /*clazz*/,
                                     jlong handle) {
    if (handle != 0L) {
        rft_close(fromHandle(handle));
    }
}

JNIEXPORT jint JNICALL
Java_com_raft_HybridRaft_nativePut(JNIEnv* env, jclass /*clazz*/,
                                   jlong handle,
                                   jbyteArray key, jint keyLen,
                                   jbyteArray value, jint valueLen) {
    RaftDb* db = fromHandle(handle);
    if (db == nullptr) {
        return -1;
    }
    auto keyBuf = readByteArray(env, key, keyLen);
    auto valueBuf = readByteArray(env, value, valueLen);
    return static_cast<jint>(rft_put(
        db, keyBuf.data(), keyBuf.size(), valueBuf.data(), valueBuf.size()));
}

JNIEXPORT jbyteArray JNICALL
Java_com_raft_HybridRaft_nativeGet(JNIEnv* env, jclass /*clazz*/,
                                   jlong handle,
                                   jbyteArray key, jint keyLen) {
    RaftDb* db = fromHandle(handle);
    if (db == nullptr) {
        return nullptr;
    }
    auto keyBuf = readByteArray(env, key, keyLen);

    // Phase 1: query required size (null buffer).
    size_t needed = 0;
    uint32_t code =
        rft_get(db, keyBuf.data(), keyBuf.size(), nullptr, &needed);
    if (code == RFT_ERROR_NOT_FOUND) {
        return nullptr;
    }
    if (code != RFT_ERROR_BUFFER_TOO_SMALL && code != RFT_ERROR_OK) {
        return nullptr;
    }

    // Phase 2: allocate exactly `needed` bytes and read.
    jbyteArray result = env->NewByteArray(static_cast<jsize>(needed));
    if (result == nullptr) {
        return nullptr;
    }
    if (needed == 0) {
        // Empty value — explicit branch so we don't dereference into a
        // zero-length vector.
        return result;
    }
    std::vector<uint8_t> outBuf(needed);
    size_t outLen = needed;
    code = rft_get(db, keyBuf.data(), keyBuf.size(), outBuf.data(), &outLen);
    if (code != RFT_ERROR_OK) {
        return nullptr;
    }
    env->SetByteArrayRegion(result, 0, static_cast<jsize>(outLen),
                            reinterpret_cast<const jbyte*>(outBuf.data()));
    return result;
}

JNIEXPORT jint JNICALL
Java_com_raft_HybridRaft_nativeDelete(JNIEnv* env, jclass /*clazz*/,
                                      jlong handle,
                                      jbyteArray key, jint keyLen) {
    RaftDb* db = fromHandle(handle);
    if (db == nullptr) {
        return -1;
    }
    auto keyBuf = readByteArray(env, key, keyLen);
    return static_cast<jint>(
        rft_delete(db, keyBuf.data(), keyBuf.size()));
}

} // extern "C"
