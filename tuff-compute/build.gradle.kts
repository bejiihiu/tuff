plugins {
    `java-library`
}

// this module is deliberately plain java: the compute engine must not know anything about
// minecraft/bukkit, so no paper or server dependencies are allowed in here. ever.
dependencies {
    // jspecify nullness annotations, same flavour the rest of the repo uses (@NullMarked packages)
    api("org.jspecify:jspecify:1.0.0")

    testImplementation(platform("org.junit:junit-bom:6.0.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
