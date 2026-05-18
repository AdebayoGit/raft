// JNI shim — bridges Kotlin `external fun native*` declarations on
// `com.raftdb.RaftDb` (and friends) to the flat `rft_*` C ABI exported
// by the prebuilt `libraftdb.so`.
//
// Library layout:
//   libraftdb.so         <- Rust core (cdylib of `raft-db`)
//   libraftdb-jni.so     <- this shim, links against libraftdb.so
//
// Kotlin loads both via `System.loadLibrary`. The dynamic linker pulls
// in `libraftdb.so` because this shim depends on it.

#include <jni.h>
#include <stdint.h>
#include <string.h>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

// ── C ABI declarations (mirroring core/include/raft.h) ────────────────

extern "C" {

typedef struct RaftDb RaftDb;
typedef struct RaftQueryResult RaftQueryResult;
typedef struct RaftTransaction RaftTransaction;
typedef void (*RftObserveCallback)(const char* event_json, void* user_data);

RaftDb* rft_open(const char* path, uint32_t* out_err);
void rft_close(RaftDb* db);

uint32_t rft_put(RaftDb* db, const uint8_t* key, size_t key_len,
                 const uint8_t* value, size_t value_len);
uint32_t rft_get(RaftDb* db, const uint8_t* key, size_t key_len,
                 uint8_t* out_value, size_t* out_len);
uint32_t rft_delete(RaftDb* db, const uint8_t* key, size_t key_len);

uint32_t rft_collection_put(RaftDb* db, const char* collection,
                            const uint8_t* doc_json, size_t doc_json_len);
uint32_t rft_collection_put_auto(RaftDb* db, const char* collection,
                                 const uint8_t* doc_json, size_t doc_json_len,
                                 uint64_t* out_doc_id);
uint32_t rft_collection_get(RaftDb* db, const char* collection, uint64_t doc_id,
                            uint8_t* out_buf, size_t* out_len);
uint32_t rft_collection_delete(RaftDb* db, const char* collection, uint64_t doc_id);
uint32_t rft_collection_count(RaftDb* db, const char* collection, size_t* out_count);
uint32_t rft_collection_list_ids(RaftDb* db, const char* collection,
                                 uint64_t* out_ids, size_t* out_len);

uint32_t rft_query_execute(RaftDb* db, const uint8_t* query_json, size_t query_json_len,
                           RaftQueryResult** out_result);
size_t rft_query_result_count(const RaftQueryResult* result);
uint32_t rft_query_result_get(const RaftQueryResult* result, size_t index,
                              uint8_t* out_buf, size_t* out_len);
void rft_query_result_free(RaftQueryResult* result);

uint32_t rft_transaction_begin(RaftDb* db, RaftTransaction** out_txn);
uint32_t rft_transaction_get(RaftTransaction* txn, const char* collection, uint64_t doc_id,
                             uint8_t* out_buf, size_t* out_len);
uint32_t rft_transaction_put(RaftTransaction* txn, const char* collection,
                             const uint8_t* doc_json, size_t doc_json_len);
uint32_t rft_transaction_delete(RaftTransaction* txn, const char* collection, uint64_t doc_id);
uint32_t rft_transaction_commit(RaftTransaction* txn);
void rft_transaction_rollback(RaftTransaction* txn);

uint32_t rft_observe(RaftDb* db, const char* collection,
                     RftObserveCallback callback, void* user_data,
                     uint64_t* out_sub_id);
uint32_t rft_observe_query(RaftDb* db, const uint8_t* query_json, size_t query_json_len,
                           RftObserveCallback callback, void* user_data,
                           uint64_t* out_sub_id);
uint32_t rft_unobserve(RaftDb* db, uint64_t sub_id);

} // extern "C"

// ── Error code helpers ────────────────────────────────────────────────

constexpr uint32_t RFT_ERROR_OK = 0;
constexpr uint32_t RFT_ERROR_NOT_FOUND = 4;
constexpr uint32_t RFT_ERROR_BUFFER_TOO_SMALL = 5;

// ── JavaVM cache for callbacks ────────────────────────────────────────

namespace {

JavaVM* g_vm = nullptr;

// Each subscription holds a global ref to a Kotlin callback object
// implementing `void onEvent(String json)`. The C trampoline below
// receives `user_data` as a pointer to one of these, calls the Kotlin
// method synchronously on a tokio thread (attached to the JVM as needed).
struct ObserverContext {
    jobject callback;       // Global ref to Kotlin RaftObserverCallback
    jmethodID onEventMethod; // void onEvent(String)
};

// All live observer contexts, keyed by raw pointer (the same pointer
// stored in `user_data` on the Rust side). The map owns the contexts so
// we can free them in `nativeUnobserve` regardless of where it's called.
std::mutex g_observersMutex;
std::unordered_map<uintptr_t, ObserverContext*> g_observers;

} // namespace

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* /*reserved*/) {
    g_vm = vm;
    return JNI_VERSION_1_6;
}

namespace {

// Read a Java byte[] into a contiguous std::vector<uint8_t>.
std::vector<uint8_t> readByteArray(JNIEnv* env, jbyteArray arr) {
    if (arr == nullptr) return {};
    jsize len = env->GetArrayLength(arr);
    std::vector<uint8_t> buf(static_cast<size_t>(len));
    if (len > 0) {
        env->GetByteArrayRegion(arr, 0, len,
                                reinterpret_cast<jbyte*>(buf.data()));
    }
    return buf;
}

// Build a Java byte[] from a buffer + length.
jbyteArray makeByteArray(JNIEnv* env, const uint8_t* data, size_t len) {
    jbyteArray result = env->NewByteArray(static_cast<jsize>(len));
    if (result == nullptr || len == 0) return result;
    env->SetByteArrayRegion(result, 0, static_cast<jsize>(len),
                            reinterpret_cast<const jbyte*>(data));
    return result;
}

// Two-phase read: query required size, allocate, read. Returns null on
// not-found, throws nothing — caller is responsible for code != OK.
// On error, returns nullptr and writes the error code via *outCode.
jbyteArray readTwoPhase(JNIEnv* env,
                        uint32_t (*reader)(void*, void*, uint8_t*, size_t*),
                        void* arg1, void* arg2,
                        uint32_t* outCode) {
    size_t needed = 0;
    uint32_t sizeCode = reader(arg1, arg2, nullptr, &needed);
    if (sizeCode == RFT_ERROR_NOT_FOUND) {
        *outCode = RFT_ERROR_OK;
        return nullptr;
    }
    if (sizeCode != RFT_ERROR_BUFFER_TOO_SMALL && sizeCode != RFT_ERROR_OK) {
        *outCode = sizeCode;
        return nullptr;
    }
    std::vector<uint8_t> buf(needed);
    size_t actual = needed;
    uint32_t readCode = reader(arg1, arg2, buf.data(), &actual);
    if (readCode != RFT_ERROR_OK) {
        *outCode = readCode;
        return nullptr;
    }
    *outCode = RFT_ERROR_OK;
    return makeByteArray(env, buf.data(), actual);
}

} // namespace

// ── JNI exports — Raw KV ──────────────────────────────────────────────

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_raftdb_RaftDb_nativeOpen(JNIEnv* env, jclass /*clazz*/, jstring path) {
    const char* cPath = env->GetStringUTFChars(path, nullptr);
    if (cPath == nullptr) return 0L;
    uint32_t err = 0;
    RaftDb* db = rft_open(cPath, &err);
    env->ReleaseStringUTFChars(path, cPath);
    if (err != RFT_ERROR_OK || db == nullptr) return 0L;
    return reinterpret_cast<jlong>(db);
}

JNIEXPORT void JNICALL
Java_com_raftdb_RaftDb_nativeClose(JNIEnv* /*env*/, jclass /*clazz*/, jlong handle) {
    rft_close(reinterpret_cast<RaftDb*>(handle));
}

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativePut(JNIEnv* env, jclass /*clazz*/,
                                 jlong handle,
                                 jbyteArray key, jint /*keyLen*/,
                                 jbyteArray value, jint /*valueLen*/) {
    auto keyBuf = readByteArray(env, key);
    auto valBuf = readByteArray(env, value);
    return static_cast<jint>(rft_put(
        reinterpret_cast<RaftDb*>(handle),
        keyBuf.data(), keyBuf.size(),
        valBuf.data(), valBuf.size()));
}

JNIEXPORT jbyteArray JNICALL
Java_com_raftdb_RaftDb_nativeGet(JNIEnv* env, jclass /*clazz*/,
                                 jlong handle, jbyteArray key, jint /*keyLen*/) {
    auto keyBuf = readByteArray(env, key);
    RaftDb* db = reinterpret_cast<RaftDb*>(handle);

    size_t needed = 0;
    uint32_t sizeCode = rft_get(db, keyBuf.data(), keyBuf.size(), nullptr, &needed);
    if (sizeCode == RFT_ERROR_NOT_FOUND) return nullptr;
    if (sizeCode != RFT_ERROR_BUFFER_TOO_SMALL && sizeCode != RFT_ERROR_OK) {
        return nullptr; // Kotlin layer surfaces only `null` for not-found;
                        // other errors are silently absent for now (matches
                        // prior shim). Future: throw via JNI exception.
    }
    std::vector<uint8_t> buf(needed);
    size_t actual = needed;
    uint32_t readCode = rft_get(db, keyBuf.data(), keyBuf.size(), buf.data(), &actual);
    if (readCode != RFT_ERROR_OK) return nullptr;
    return makeByteArray(env, buf.data(), actual);
}

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeDelete(JNIEnv* env, jclass /*clazz*/,
                                    jlong handle, jbyteArray key, jint /*keyLen*/) {
    auto keyBuf = readByteArray(env, key);
    return static_cast<jint>(rft_delete(
        reinterpret_cast<RaftDb*>(handle),
        keyBuf.data(), keyBuf.size()));
}

// ── Typed Collections ─────────────────────────────────────────────────

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeCollectionPut(JNIEnv* env, jclass /*clazz*/,
                                           jlong handle, jstring collection,
                                           jbyteArray docJson) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    auto json = readByteArray(env, docJson);
    jint code = static_cast<jint>(rft_collection_put(
        reinterpret_cast<RaftDb*>(handle),
        cName, json.data(), json.size()));
    env->ReleaseStringUTFChars(collection, cName);
    return code;
}

JNIEXPORT jlong JNICALL
Java_com_raftdb_RaftDb_nativeCollectionPutAuto(JNIEnv* env, jclass /*clazz*/,
                                               jlong handle, jstring collection,
                                               jbyteArray docJson,
                                               jintArray outCode) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    auto json = readByteArray(env, docJson);
    uint64_t docId = 0;
    uint32_t code = rft_collection_put_auto(
        reinterpret_cast<RaftDb*>(handle),
        cName, json.data(), json.size(), &docId);
    env->ReleaseStringUTFChars(collection, cName);
    jint c = static_cast<jint>(code);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    return static_cast<jlong>(docId);
}

JNIEXPORT jbyteArray JNICALL
Java_com_raftdb_RaftDb_nativeCollectionGet(JNIEnv* env, jclass /*clazz*/,
                                           jlong handle, jstring collection,
                                           jlong docId, jintArray outCode) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    RaftDb* db = reinterpret_cast<RaftDb*>(handle);

    size_t needed = 0;
    uint32_t sizeCode = rft_collection_get(db, cName, static_cast<uint64_t>(docId),
                                           nullptr, &needed);
    if (sizeCode == RFT_ERROR_NOT_FOUND) {
        env->ReleaseStringUTFChars(collection, cName);
        jint ok = RFT_ERROR_OK;
        env->SetIntArrayRegion(outCode, 0, 1, &ok);
        return nullptr;
    }
    if (sizeCode != RFT_ERROR_BUFFER_TOO_SMALL && sizeCode != RFT_ERROR_OK) {
        env->ReleaseStringUTFChars(collection, cName);
        jint c = static_cast<jint>(sizeCode);
        env->SetIntArrayRegion(outCode, 0, 1, &c);
        return nullptr;
    }
    std::vector<uint8_t> buf(needed);
    size_t actual = needed;
    uint32_t readCode = rft_collection_get(db, cName, static_cast<uint64_t>(docId),
                                           buf.data(), &actual);
    env->ReleaseStringUTFChars(collection, cName);
    jint c = static_cast<jint>(readCode);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    if (readCode != RFT_ERROR_OK) return nullptr;
    return makeByteArray(env, buf.data(), actual);
}

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeCollectionDelete(JNIEnv* env, jclass /*clazz*/,
                                              jlong handle, jstring collection,
                                              jlong docId) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    jint code = static_cast<jint>(rft_collection_delete(
        reinterpret_cast<RaftDb*>(handle), cName,
        static_cast<uint64_t>(docId)));
    env->ReleaseStringUTFChars(collection, cName);
    return code;
}

JNIEXPORT jlong JNICALL
Java_com_raftdb_RaftDb_nativeCollectionCount(JNIEnv* env, jclass /*clazz*/,
                                             jlong handle, jstring collection,
                                             jintArray outCode) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    size_t count = 0;
    uint32_t code = rft_collection_count(
        reinterpret_cast<RaftDb*>(handle), cName, &count);
    env->ReleaseStringUTFChars(collection, cName);
    jint c = static_cast<jint>(code);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    return static_cast<jlong>(count);
}

JNIEXPORT jlongArray JNICALL
Java_com_raftdb_RaftDb_nativeCollectionListIds(JNIEnv* env, jclass /*clazz*/,
                                               jlong handle, jstring collection,
                                               jintArray outCode) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    RaftDb* db = reinterpret_cast<RaftDb*>(handle);

    size_t needed = 0;
    uint32_t sizeCode = rft_collection_list_ids(db, cName, nullptr, &needed);
    if (sizeCode != RFT_ERROR_BUFFER_TOO_SMALL && sizeCode != RFT_ERROR_OK) {
        env->ReleaseStringUTFChars(collection, cName);
        jint c = static_cast<jint>(sizeCode);
        env->SetIntArrayRegion(outCode, 0, 1, &c);
        return nullptr;
    }
    if (needed == 0) {
        env->ReleaseStringUTFChars(collection, cName);
        jint ok = RFT_ERROR_OK;
        env->SetIntArrayRegion(outCode, 0, 1, &ok);
        return env->NewLongArray(0);
    }
    std::vector<uint64_t> ids(needed);
    size_t actual = needed;
    uint32_t readCode = rft_collection_list_ids(db, cName, ids.data(), &actual);
    env->ReleaseStringUTFChars(collection, cName);
    jint c = static_cast<jint>(readCode);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    if (readCode != RFT_ERROR_OK) return nullptr;

    jlongArray result = env->NewLongArray(static_cast<jsize>(actual));
    if (result == nullptr) return nullptr;
    // jlong and uint64_t have the same width on supported Android ABIs;
    // copy via a temporary jlong buffer to avoid undefined sign mismatch.
    std::vector<jlong> tmp(actual);
    for (size_t i = 0; i < actual; i++) tmp[i] = static_cast<jlong>(ids[i]);
    env->SetLongArrayRegion(result, 0, static_cast<jsize>(actual), tmp.data());
    return result;
}

// ── Queries ───────────────────────────────────────────────────────────

JNIEXPORT jobjectArray JNICALL
Java_com_raftdb_RaftDb_nativeQueryExecute(JNIEnv* env, jclass /*clazz*/,
                                          jlong handle, jbyteArray queryJson,
                                          jintArray outCode) {
    auto json = readByteArray(env, queryJson);
    RaftDb* db = reinterpret_cast<RaftDb*>(handle);
    RaftQueryResult* result = nullptr;
    uint32_t execCode = rft_query_execute(db, json.data(), json.size(), &result);
    if (execCode != RFT_ERROR_OK) {
        jint c = static_cast<jint>(execCode);
        env->SetIntArrayRegion(outCode, 0, 1, &c);
        return nullptr;
    }
    if (result == nullptr) {
        jint ok = RFT_ERROR_OK;
        env->SetIntArrayRegion(outCode, 0, 1, &ok);
        jclass byteArrayClass = env->FindClass("[B");
        return env->NewObjectArray(0, byteArrayClass, nullptr);
    }

    size_t count = rft_query_result_count(result);
    jclass byteArrayClass = env->FindClass("[B");
    jobjectArray docs = env->NewObjectArray(static_cast<jsize>(count),
                                            byteArrayClass, nullptr);
    for (size_t i = 0; i < count; i++) {
        size_t needed = 0;
        uint32_t sizeCode = rft_query_result_get(result, i, nullptr, &needed);
        if (sizeCode != RFT_ERROR_BUFFER_TOO_SMALL && sizeCode != RFT_ERROR_OK) {
            rft_query_result_free(result);
            jint c = static_cast<jint>(sizeCode);
            env->SetIntArrayRegion(outCode, 0, 1, &c);
            return nullptr;
        }
        std::vector<uint8_t> buf(needed);
        size_t actual = needed;
        uint32_t readCode = rft_query_result_get(result, i, buf.data(), &actual);
        if (readCode != RFT_ERROR_OK) {
            rft_query_result_free(result);
            jint c = static_cast<jint>(readCode);
            env->SetIntArrayRegion(outCode, 0, 1, &c);
            return nullptr;
        }
        jbyteArray doc = makeByteArray(env, buf.data(), actual);
        env->SetObjectArrayElement(docs, static_cast<jsize>(i), doc);
        env->DeleteLocalRef(doc);
    }
    rft_query_result_free(result);
    jint ok = RFT_ERROR_OK;
    env->SetIntArrayRegion(outCode, 0, 1, &ok);
    return docs;
}

// ── Transactions ──────────────────────────────────────────────────────

JNIEXPORT jlong JNICALL
Java_com_raftdb_RaftDb_nativeTransactionBegin(JNIEnv* env, jclass /*clazz*/,
                                              jlong handle, jintArray outCode) {
    RaftTransaction* txn = nullptr;
    uint32_t code = rft_transaction_begin(reinterpret_cast<RaftDb*>(handle), &txn);
    jint c = static_cast<jint>(code);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    return reinterpret_cast<jlong>(txn);
}

JNIEXPORT jbyteArray JNICALL
Java_com_raftdb_RaftDb_nativeTransactionGet(JNIEnv* env, jclass /*clazz*/,
                                            jlong txnHandle, jstring collection,
                                            jlong docId, jintArray outCode) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    RaftTransaction* txn = reinterpret_cast<RaftTransaction*>(txnHandle);
    size_t needed = 0;
    uint32_t sizeCode = rft_transaction_get(txn, cName, static_cast<uint64_t>(docId),
                                            nullptr, &needed);
    if (sizeCode == RFT_ERROR_NOT_FOUND) {
        env->ReleaseStringUTFChars(collection, cName);
        jint ok = RFT_ERROR_OK;
        env->SetIntArrayRegion(outCode, 0, 1, &ok);
        return nullptr;
    }
    if (sizeCode != RFT_ERROR_BUFFER_TOO_SMALL && sizeCode != RFT_ERROR_OK) {
        env->ReleaseStringUTFChars(collection, cName);
        jint c = static_cast<jint>(sizeCode);
        env->SetIntArrayRegion(outCode, 0, 1, &c);
        return nullptr;
    }
    std::vector<uint8_t> buf(needed);
    size_t actual = needed;
    uint32_t readCode = rft_transaction_get(txn, cName, static_cast<uint64_t>(docId),
                                            buf.data(), &actual);
    env->ReleaseStringUTFChars(collection, cName);
    jint c = static_cast<jint>(readCode);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    if (readCode != RFT_ERROR_OK) return nullptr;
    return makeByteArray(env, buf.data(), actual);
}

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeTransactionPut(JNIEnv* env, jclass /*clazz*/,
                                            jlong txnHandle, jstring collection,
                                            jbyteArray docJson) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    auto json = readByteArray(env, docJson);
    jint code = static_cast<jint>(rft_transaction_put(
        reinterpret_cast<RaftTransaction*>(txnHandle),
        cName, json.data(), json.size()));
    env->ReleaseStringUTFChars(collection, cName);
    return code;
}

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeTransactionDelete(JNIEnv* env, jclass /*clazz*/,
                                               jlong txnHandle, jstring collection,
                                               jlong docId) {
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    jint code = static_cast<jint>(rft_transaction_delete(
        reinterpret_cast<RaftTransaction*>(txnHandle), cName,
        static_cast<uint64_t>(docId)));
    env->ReleaseStringUTFChars(collection, cName);
    return code;
}

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeTransactionCommit(JNIEnv* /*env*/, jclass /*clazz*/,
                                               jlong txnHandle) {
    return static_cast<jint>(rft_transaction_commit(
        reinterpret_cast<RaftTransaction*>(txnHandle)));
}

JNIEXPORT void JNICALL
Java_com_raftdb_RaftDb_nativeTransactionRollback(JNIEnv* /*env*/, jclass /*clazz*/,
                                                 jlong txnHandle) {
    rft_transaction_rollback(reinterpret_cast<RaftTransaction*>(txnHandle));
}

} // extern "C"

// ── Observe — JNI callback dance ──────────────────────────────────────

namespace {

// C-side trampoline. Called synchronously from the Rust tokio thread.
// `user_data` is a pointer to an ObserverContext we allocated and
// pushed into g_observers.
void observeTrampoline(const char* event_json, void* user_data) {
    if (g_vm == nullptr || user_data == nullptr || event_json == nullptr) return;
    auto* ctx = reinterpret_cast<ObserverContext*>(user_data);

    JNIEnv* env = nullptr;
    bool attached = false;
    int getEnvResult = g_vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);
    if (getEnvResult == JNI_EDETACHED) {
        if (g_vm->AttachCurrentThread(&env, nullptr) != JNI_OK) return;
        attached = true;
    } else if (getEnvResult != JNI_OK) {
        return;
    }

    jstring jstr = env->NewStringUTF(event_json);
    if (jstr != nullptr) {
        env->CallVoidMethod(ctx->callback, ctx->onEventMethod, jstr);
        env->DeleteLocalRef(jstr);
    }
    // Clear any exception the callback may have raised so subsequent
    // events don't fail.
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }

    if (attached) {
        g_vm->DetachCurrentThread();
    }
}

// Allocate an ObserverContext, register it, return the pointer.
ObserverContext* registerObserver(JNIEnv* env, jobject callback) {
    jclass cbClass = env->GetObjectClass(callback);
    jmethodID method = env->GetMethodID(cbClass, "onEvent",
                                        "(Ljava/lang/String;)V");
    if (method == nullptr) return nullptr;
    auto* ctx = new ObserverContext{
        env->NewGlobalRef(callback),
        method,
    };
    {
        std::lock_guard<std::mutex> lock(g_observersMutex);
        g_observers.emplace(reinterpret_cast<uintptr_t>(ctx), ctx);
    }
    return ctx;
}

// Look up + free an observer context by its raw pointer. Called when
// a subscription is being torn down.
void freeObserver(JNIEnv* env, ObserverContext* ctx) {
    if (ctx == nullptr) return;
    {
        std::lock_guard<std::mutex> lock(g_observersMutex);
        g_observers.erase(reinterpret_cast<uintptr_t>(ctx));
    }
    if (ctx->callback != nullptr) {
        env->DeleteGlobalRef(ctx->callback);
    }
    delete ctx;
}

} // namespace

extern "C" {

JNIEXPORT jint JNICALL
Java_com_raftdb_RaftDb_nativeUnobserve(JNIEnv* env, jclass /*clazz*/,
                                       jlong handle, jlong subId,
                                       jlong ctxAddr) {
    uint32_t code = rft_unobserve(reinterpret_cast<RaftDb*>(handle),
                                  static_cast<uint64_t>(subId));
    auto* ctx = reinterpret_cast<ObserverContext*>(static_cast<uintptr_t>(ctxAddr));
    if (ctx != nullptr) {
        freeObserver(env, ctx);
    }
    return static_cast<jint>(code);
}

// nativeObserveCollection and nativeObserveQueryHandle both return a
// `long[2]` of `{ subId, ctxAddr }`. Kotlin stores both and passes
// them back to `nativeUnobserve` for cleanup.

JNIEXPORT jlongArray JNICALL
Java_com_raftdb_RaftDb_nativeObserveCollection(JNIEnv* env, jclass /*clazz*/,
                                               jlong handle, jstring collection,
                                               jobject callback, jintArray outCode) {
    auto* ctx = registerObserver(env, callback);
    if (ctx == nullptr) {
        jint err = static_cast<jint>(2);
        env->SetIntArrayRegion(outCode, 0, 1, &err);
        return nullptr;
    }
    const char* cName = env->GetStringUTFChars(collection, nullptr);
    uint64_t subId = 0;
    uint32_t code = rft_observe(reinterpret_cast<RaftDb*>(handle), cName,
                                observeTrampoline, ctx, &subId);
    env->ReleaseStringUTFChars(collection, cName);
    jint c = static_cast<jint>(code);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    if (code != RFT_ERROR_OK) {
        freeObserver(env, ctx);
        return nullptr;
    }
    jlongArray result = env->NewLongArray(2);
    jlong arr[2] = {
        static_cast<jlong>(subId),
        static_cast<jlong>(reinterpret_cast<uintptr_t>(ctx)),
    };
    env->SetLongArrayRegion(result, 0, 2, arr);
    return result;
}

JNIEXPORT jlongArray JNICALL
Java_com_raftdb_RaftDb_nativeObserveQueryHandle(JNIEnv* env, jclass /*clazz*/,
                                                jlong handle, jbyteArray queryJson,
                                                jobject callback, jintArray outCode) {
    auto* ctx = registerObserver(env, callback);
    if (ctx == nullptr) {
        jint err = static_cast<jint>(2);
        env->SetIntArrayRegion(outCode, 0, 1, &err);
        return nullptr;
    }
    auto json = readByteArray(env, queryJson);
    uint64_t subId = 0;
    uint32_t code = rft_observe_query(reinterpret_cast<RaftDb*>(handle),
                                      json.data(), json.size(),
                                      observeTrampoline, ctx, &subId);
    jint c = static_cast<jint>(code);
    env->SetIntArrayRegion(outCode, 0, 1, &c);
    if (code != RFT_ERROR_OK) {
        freeObserver(env, ctx);
        return nullptr;
    }
    jlongArray result = env->NewLongArray(2);
    jlong arr[2] = {
        static_cast<jlong>(subId),
        static_cast<jlong>(reinterpret_cast<uintptr_t>(ctx)),
    };
    env->SetLongArrayRegion(result, 0, 2, arr);
    return result;
}

} // extern "C"
