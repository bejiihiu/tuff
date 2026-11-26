pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenLocal()
        maven("https://repo.papermc.io/repository/maven-public/")
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "tuff"

include("tuff-api")
include("tuff-server")
include("tuff-checkstyle")
include("tuff-compute")
include("tuff-threads")
include("tuff-benchmarks")

gradle.lifecycle.beforeProject {
    val mcVersion = providers.gradleProperty("mcVersion").get().trim()
    val tuffVersionChannel = providers.gradleProperty("channel").get().trim()
    val tuffBuildNumber = providers.environmentVariable("BUILD_NUMBER").orNull?.trim()?.toInt()
    val versionString = if (tuffBuildNumber == null) {
        "$mcVersion.local-SNAPSHOT"
    } else {
        "$mcVersion.build.$tuffBuildNumber-${tuffVersionChannel.lowercase()}"
    }
    version = versionString
}
