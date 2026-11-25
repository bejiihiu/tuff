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
