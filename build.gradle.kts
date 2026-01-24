plugins {
    id("java")
    id("com.diffplug.spotless") version "6.25.0"
}

group = "com.github.rockymine"
version = "1.1.0"

repositories {
    mavenCentral()
    maven("https://repo.pgm.fyi/snapshots")
}

dependencies {
    compileOnly("app.ashcon:sportpaper:1.8.8-R0.1-SNAPSHOT")
    compileOnly("tc.oc.pgm:core:0.16-SNAPSHOT")
    implementation("blue.strategic.parquet:parquet-floor:1.51")
    implementation("com.squareup.okhttp3:okhttp:5.3.0")
}

// Bundle parquet-floor into the JAR
tasks.jar {
    from(configurations.runtimeClasspath.get().map {
        if (it.isDirectory) it else zipTree(it)
    })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from("src/main/resources")
}

spotless {
    ratchetFrom = "origin/main"
    java {
        removeUnusedImports()
        palantirJavaFormat("2.73.0").style("GOOGLE").formatJavadoc(true)
    }
}
