project(raftdb-jni)
cmake_minimum_required(VERSION 3.18)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_VERBOSE_MAKEFILE ON)

# The Rust core is shipped as a pre-built `.so` per ABI under jniLibs.
# Import it so the JNI shim can link against it.
add_library(raftdb SHARED IMPORTED)
set_target_properties(raftdb PROPERTIES
    IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/../jniLibs/${ANDROID_ABI}/libraftdb.so"
)

# The shim — exports `Java_com_raftdb_RaftDb_native*` symbols that
# translate JNI calls into the flat `rft_*` C ABI.
add_library(raftdb-jni SHARED
    raft-jni.cpp
)

find_library(LOG_LIB log)

target_include_directories(raftdb-jni PRIVATE
    "${CMAKE_SOURCE_DIR}"
)

target_link_libraries(raftdb-jni
    ${LOG_LIB}
    android
    raftdb
)
