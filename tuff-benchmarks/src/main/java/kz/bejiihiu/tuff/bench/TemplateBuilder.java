package kz.bejiihiu.tuff.bench;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Builds the shared server template once per jar and block identity.
 *
 * <p>The template is a stopped server directory: generated world, downloaded libraries and the
 * generated config files. Every run copies it, so the world is byte identical across arms and no run
 * pays for chunk generation again. Rebuilding is keyed on the jar hash, not on a date, so a rebuild is
 * automatic when the server changes and never happens for a mere rerun.
 *
 * <p>Build time is not measurement time. The template boot is captured in {@code template-boot.log}
 * next to the template, and its failure stops the harness: measuring against a half generated world
 * would be worse than not measuring.
 */
final class TemplateBuilder {

    private static final Duration BOOT_TIMEOUT = Duration.ofMinutes(10L);
    private static final Duration SETTLE = Duration.ofSeconds(15L);

    private static final List<String> TEMPLATE_SETUP = List.of(
        "gamerule doMobSpawning false",
        "gamerule doDaylightCycle false",
        "gamerule doWeatherCycle false",
        "gamerule randomTickSpeed 0",
        "time set 6000"
    );

    private TemplateBuilder() {
    }

    /**
     * Ensures the template exists for this jar.
     *
     * @param options  harness options
     * @param identity block identity
     * @return the template directory
     * @throws IOException          if the template cannot be built
     * @throws InterruptedException if the build thread is interrupted
     */
    static Path ensure(final BenchOptions options, final Identity identity) throws IOException, InterruptedException {
        final Path template = options.outRoot.toAbsolutePath().resolve("template");
        final Path marker = template.resolve(".template.json");
        if (options.rebuildTemplate) {
            Files.deleteIfExists(marker);
        }
        if (isCurrent(marker, identity) && Files.isDirectory(template.resolve("world"))) {
            return template;
        }
        // the downloaded libraries and the patched vanilla jar do not depend on the world; keeping
        // them makes a rebuild after a new jar cheap and works offline once they exist
        deleteTree(template.resolve("world"));
        deleteTree(template.resolve("config"));
        deleteTree(template.resolve("logs"));
        deleteTree(template.resolve("jfr"));
        deleteTree(template.resolve("plugins"));
        Files.deleteIfExists(template.resolve("gc.log"));
        Files.deleteIfExists(template.resolve("server.log"));
        Files.deleteIfExists(template.resolve("template-boot.log"));
        Files.deleteIfExists(template.resolve("usercache.json"));
        Files.deleteIfExists(template.resolve(".template.json"));
        Files.createDirectories(template);
        System.out.println("[bench] building the server template from " + identity.jarPath());
        ServerConfig.writeServerFiles(template, options.port, "1337");
        ServerConfig.writeTuffYml(template, Arm.A, -1, -1, 4, false);
        final Path bootLog = template.resolve("template-boot.log");
        final List<String> command = new ArrayList<>();
        command.add(BenchOptions.javaExecutable().toString());
        command.addAll(BenchOptions.jvmFlags(template, options.heap, options.extraJvm));
        command.add("-jar");
        command.add(identity.jarPath());
        command.add("--nogui");
        final Process process = new ProcessBuilder(command)
            .directory(template.toFile())
            .redirectErrorStream(true)
            .start();
        final java.util.Deque<String> tail = new java.util.ArrayDeque<>();
        final Object monitor = new Object();
        final Thread pump = Thread.ofPlatform().daemon(true).name("tuff-bench-template").start(() -> {
            try (
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
                );
                BufferedWriter writer = Files.newBufferedWriter(bootLog, StandardCharsets.UTF_8)
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                    writer.flush();
                    synchronized (monitor) {
                        tail.addLast(line);
                        if (tail.size() > 2_000) {
                            tail.removeFirst();
                        }
                        monitor.notifyAll();
                    }
                }
            } catch (final IOException failure) {
                synchronized (monitor) {
                    tail.addLast("[bench] template log pump failed: " + failure.getMessage());
                    monitor.notifyAll();
                }
            }
        });
        try {
            if (!await(tail, monitor, "Done (", BOOT_TIMEOUT)) {
                throw new IOException("template boot did not reach 'Done ('; see " + bootLog);
            }
            final LoadSpec geometry = new LoadSpec(4, options.areaChunks, options.areaStep, 0, options.entityType);
            try (BufferedWriter console = new BufferedWriter(
                new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)
            )) {
                for (final String setup : TEMPLATE_SETUP) {
                    console.write(setup);
                    console.write('\n');
                }
                for (int area = 0; area < 4; ++area) {
                    console.write(geometry.forceLoadCommand(area));
                    console.write('\n');
                }
                console.flush();
                Thread.sleep(SETTLE.toMillis());
                // the world keeps the generated chunks, but not the tickets: every run owns its own
                // force-loaded set and a template ticket would silently add a region to every arm
                console.write("forceload remove all\n");
                console.write("kill @e[type=!player]\n");
                console.flush();
                Thread.sleep(2_000L);
                console.write("stop\n");
                console.flush();
            }
            if (!process.waitFor(BOOT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new IOException("template did not stop within " + BOOT_TIMEOUT + "; see " + bootLog);
            }
            if (process.exitValue() != 0) {
                throw new IOException("template boot failed with exit " + process.exitValue() + "; see " + bootLog);
            }
        } finally {
            pump.join(5_000L);
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        }
        ServerConfig.patchPaperGlobal(template, Arm.A, -1, 4);
        // the template must not smuggle one run's measurements into the next one
        deleteTree(template.resolve("logs"));
        deleteTree(template.resolve("jfr"));
        Files.deleteIfExists(template.resolve("gc.log"));
        Files.deleteIfExists(template.resolve("server.log"));
        Json.write(marker, Map.of(
            "jar_sha256", identity.jarSha(),
            "patchset_hash", identity.patchDigest(),
            "jdk", identity.jdk(),
            "world", "flat-1337"
        ));
        return template;
    }

    private static boolean await(
        final java.util.Deque<String> tail,
        final Object monitor,
        final String needle,
        final Duration timeout
    ) throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (monitor) {
            while (true) {
                for (final String line : tail) {
                    if (line.contains(needle)) {
                        return true;
                    }
                }
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                monitor.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
        }
    }

    private static boolean isCurrent(final Path marker, final Identity identity) {
        if (!Files.isRegularFile(marker)) {
            return false;
        }
        try {
            final Object parsed = Json.read(marker);
            if (!(parsed instanceof final Map<?, ?> map)) {
                return false;
            }
            return identity.jarSha().equals(map.get("jar_sha256"))
                && identity.patchDigest().equals(map.get("patchset_hash"));
        } catch (final IOException failure) {
            return false;
        }
    }

    /**
     * Deletes a directory tree, tolerating files that a previous process still holds.
     *
     * @param root directory to delete
     * @throws IOException if a file cannot be deleted
     */
    static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            for (final Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
