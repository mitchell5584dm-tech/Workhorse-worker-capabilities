plugins {
    id("com.gradle.develocity") version "4.3.1"
}

// Named explicitly so the build doesn't depend on the checkout folder name (this fork is
// "Workhorse-worker-capabilities"); testapp/build.gradle.kts looks it up by this name.
includeBuild("..") {
    name = "gradle-play-publisher"
}
