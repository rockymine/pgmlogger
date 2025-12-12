plugins {
    id("java")
}

group = "com.example"
version = "1.0.0"

repositories {
    mavenCentral()
    maven("https://repo.pgm.fyi/snapshots")
}

dependencies {
    compileOnly("app.ashcon:sportpaper:1.8.8-R0.1-SNAPSHOT")
    compileOnly("tc.oc.pgm:core:0.16-SNAPSHOT")
    implementation("blue.strategic.parquet:parquet-floor:1.51")
    implementation("org.yaml:snakeyaml:2.2")
}

// Bundle parquet-floor into the JAR
tasks.jar {
    from(configurations.runtimeClasspath.get().map {
        if (it.isDirectory) it else zipTree(it)
    })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from("src/main/resources")
}

configurations.all {
    resolutionStrategy {
        force("org.yaml:snakeyaml:2.2")
    }
}

