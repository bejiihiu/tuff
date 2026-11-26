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
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "Tuff Threads",
            "Implementation-Version" to project.version,
        )
    }
}
