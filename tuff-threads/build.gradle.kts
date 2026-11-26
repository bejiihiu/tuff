plugins {
    `java-library`
}

// same rule as tuff-compute: this module is plain java on purpose. utmi is execution
// infrastructure, it must not know about minecraft, bukkit, paper or folia classes. ever.
// checkstyle is applied by the root build (paperweight paper-checkstyle), so there is no local
// config to drift: whatever server sources must satisfy, this module satisfies too.
dependencies {
    // jspecify nullness annotations, the flavour the rest of the repo uses (@NullMarked packages)
    api("org.jspecify:jspecify:1.0.0")

    testImplementation(platform("org.junit:junit-bom:6.0.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()

    // the raw thread guard test reads sources of sibling modules, which gradle cannot infer from the
    // classpath. without these inputs a change in compute/api/server leaves this task up to date and
    // the guard silently skips. it's a cheap file scan over a few hundred sources.
    inputs.dir(rootProject.file("tuff-compute/src/main/java"))
        .withPropertyName("guardSources.compute")
    inputs.dir(rootProject.file("tuff-api/src/main/java"))
        .withPropertyName("guardSources.api")
    inputs.dir(rootProject.file("tuff-server/src/main/java/kz/bejiihiu/tuff"))
        .withPropertyName("guardSources.server")
    inputs.dir(project.file("src/main/java"))
        .withPropertyName("guardSources.threads")
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "Tuff Threads",
            "Implementation-Version" to project.version,
        )
    }
}
