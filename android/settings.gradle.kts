// Standalone build entry point for the raftdb Android library.
//
// The module is normally consumed as `com.raftdb` from a host app, but this
// settings file lets CI (and developers) build + unit-test it in isolation:
//
//   gradle -p android assembleDebug testDebugUnitTest

pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
    plugins {
        id("com.android.library") version "8.6.1"
        id("org.jetbrains.kotlin.android") version "1.9.24"
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "raftdb"
