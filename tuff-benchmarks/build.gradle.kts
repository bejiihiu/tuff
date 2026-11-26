plugins {
    `java-library`
    id("me.champeau.jmh") version "0.7.4"
}

// benchmark-only module: it is not part of the server jar, it is not published, and the `jmh`
// task is a separate entry point that `build`/`check` never pull in.
dependencies {
    // the engine is consumed exactly like a plugin would consume it
    "jmh"(project(":tuff-compute"))
    // note: jmh 1.37 resolves asm 9.0 and still runs on jdk 25 (see benchmarks.md); no asm
    // override is needed. the only jdk 25 complaint is jmh-core touching sun.misc.Unsafe.

    // the root build adds an unversioned junit-platform-launcher to every subproject, so this
    // module needs a BOM to pin it. there is no src/test here (only src/jmh), so the `test` task
    // stays NO-SOURCE and never runs junit; this only keeps the runtime classpath resolvable.
    testRuntimeOnly(platform("org.junit:junit-bom:6.0.3"))
}

// benchmarks are not shipped and don't follow the server/paper style. checkstyleJmh would
// otherwise run the full paper ruleset over src/jmh and fail. keep the tasks in the graph but
// disabled, and give mergeCheckstyleConfigs a valid config so it doesn't fail on a missing file.
paperCheckstyle {
    directoriesToSkipFile = layout.projectDirectory.file(".checkstyle/ignored_directories.txt")
}

tasks.withType<Checkstyle>().configureEach {
    enabled = false
}

jmh {
    jmhVersion = "1.37"
    // the benchmark sources do not use the engine's test classes; keeping the test classpath out
    // also keeps the junit runtime (which the root build adds unversioned) out of the jmh classpath
    includeTests = false
    warmupIterations = 3
    iterations = 5
    fork = 1
    timeOnIteration = "1s"
    resultFormat = "JSON"
    resultsFile = layout.buildDirectory.file("results/jmh/results.json")
    humanOutputFile = layout.buildDirectory.file("results/jmh/results.txt")
    jvmArgsAppend = listOf("-Xms1g", "-Xmx1g", "-XX:+AlwaysPreTouch")
    // a benchmark that throws in a setup or teardown must fail the build, not print "? 10??"
    failOnError = true
}
