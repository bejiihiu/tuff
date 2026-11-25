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
            "Implementation-Title" to "Tuff Compute",
            "Implementation-Version" to project.version,
        )
    }
}

// tuff-api exposes this module through `api(project(":tuff-compute"))`, so it has to be on
// maven too, otherwise the published tuff-api pom points at nothing.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Tuff Compute")
                description.set("Region-aware compute scheduler engine for Tuff. Plain Java, no Minecraft or Bukkit dependencies.")
                url.set("https://github.com/bejiihiu/tuff")
                licenses {
                    license {
                        name.set("GNU General Public License v3.0")
                        url.set("https://www.gnu.org/licenses/gpl-3.0.html")
                    }
                }
                scm {
                    url.set("https://github.com/bejiihiu/tuff")
                    connection.set("scm:git:https://github.com/bejiihiu/tuff.git")
                    developerConnection.set("scm:git:ssh://git@github.com/bejiihiu/tuff.git")
                }
            }
        }
    }
}
