pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }

    plugins {
        id("io.quarkus") version "3.15.2"
    }
}

rootProject.name = "solrex"

include(":reindex")
include(":reindex-common")
include(":reindex-api")
