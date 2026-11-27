package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes the files that decide what the server does, and nothing else.
 *
 * <p>Two files matter to the experiment: {@code config/paper-global.yml} selects the region scheduler
 * and the tick thread count (Folia reads it), {@code config/tuff.yml} carries the opt-in and the fund
 * knobs (the Tuff thread layer reads it). Both are rewritten for every run from the template, so an
 * arm cannot inherit a value from the previous run.
 *
 * <p>The world and the server properties are identical across arms; only the arm's one key differs.
 * The config hashes in the manifest prove that after the fact.
 */
final class ServerConfig {

    private static final String SERVER_PROPERTIES = """
        #Minecraft server properties
        accepts-transfers=false
        allow-flight=false
        broadcast-console-to-ops=true
        broadcast-rcon-to-ops=true
        bug-report-link=
        chat-spam-threshold-seconds=10
        command-spam-threshold-seconds=10
        debug=false
        difficulty=peaceful
        enable-code-of-conduct=false
        enable-jmx-monitoring=false
        enable-query=false
        enable-rcon=false
        enable-status=true
        enforce-secure-profile=false
        enforce-whitelist=false
        entity-broadcast-range-percentage=100
        force-gamemode=false
        function-permission-level=2
        gamemode=survival
        generate-structures=false
        generator-settings={}
        hardcore=false
        hide-online-players=false
        initial-disabled-packs=
        initial-enabled-packs=vanilla
        level-name=world
        level-seed={seed}
        level-type=flat
        log-ips=false
        max-chained-neighbor-updates=1000000
        max-players=1
        max-tick-time=60000
        max-world-size=29999984
        motd=tuff-bench
        network-compression-threshold=256
        online-mode=false
        op-permission-level=4
        pause-when-empty-seconds=-1
        player-idle-timeout=0
        port={port}
        prevent-proxy-connections=false
        query.port=25565
        rate-limit=0
        rcon.password=
        rcon.port=25575
        region-file-compression=deflate
        require-resource-pack=false
        resource-pack=
        resource-pack-id=
        resource-pack-prompt=
        resource-pack-sha1=
        server-ip=
        server-port={port}
        simulation-distance=4
        spawn-protection=0
        status-heartbeat-interval=0
        sync-chunk-writes=false
        text-filtering-config=
        text-filtering-version=0
        use-native-transport=true
        view-distance=4
        white-list=false
        """;

    private ServerConfig() {
    }

    /**
     * Writes {@code eula.txt} and {@code server.properties}.
     *
     * @param runDir run directory
     * @param port   server port, unique inside a block
     * @param seed   level seed, identical for every arm
     * @throws IOException if a file cannot be written
     */
    static void writeServerFiles(final Path runDir, final int port, final String seed) throws IOException {
        Files.createDirectories(runDir);
        Files.writeString(runDir.resolve("eula.txt"), "eula=true\n", StandardCharsets.UTF_8);
        Files.writeString(
            runDir.resolve("server.properties"),
            SERVER_PROPERTIES.replace("{seed}", seed).replace("{port}", Integer.toString(port)),
            StandardCharsets.UTF_8
        );
    }

    /**
     * Writes {@code config/tuff.yml} for one arm.
     *
     * <p>Everything the run does not measure is deliberately off: the compute engine, pool management
     * and the pressure controller. The only moving parts are the region scheduler choice and, for
     * Kairos, whether the fund may grow.
     *
     * @param runDir     run directory
     * @param arm        arm to configure
     * @param threads    floor of the fund, also the EDF thread count
     * @param maxThreads ceiling for an elastic arm
     * @param gridExp    region grid exponent
     * @throws IOException if the file cannot be written
     */
    static void writeTuffYml(
        final Path runDir,
        final Arm arm,
        final int threads,
        final int maxThreads,
        final int gridExp
    ) throws IOException {
        final String yaml = """
            thread-management:
              enabled: true
              resources:
                reserve-cores: 1
              lifecycle:
                centralized-thread-factory: true
                monitor-threads: true
              region-execution:
                managed: false
                preserve-folia-scheduling: true
                scheduler: {scheduler}
                threads: {threads}
                grid-exponent: {grid}
                kairos-unstable: {unstable}
                kairos:
                  elastic: {elastic}
                  max-threads: {max}
                  keep-alive-seconds: 30
                  spawn-rate-per-second: 10
              metrics:
                enabled: true
                per-pool: true

            async-compute:
              enabled: false
            """
            .replace("{scheduler}", arm.scheduler().toLowerCase(java.util.Locale.ROOT))
            .replace("{threads}", Integer.toString(threads))
            .replace("{grid}", Integer.toString(gridExp))
            .replace("{unstable}", Boolean.toString(arm.kairosOptIn()))
            .replace("{elastic}", Boolean.toString(arm.elastic()))
            .replace("{max}", Integer.toString(arm.elastic() ? maxThreads : -1));
        final Path config = runDir.resolve("config");
        Files.createDirectories(config);
        Files.writeString(config.resolve("tuff.yml"), yaml, StandardCharsets.UTF_8);
    }

    /**
     * Rewrites the scheduler keys inside the {@code threaded-regions} block of {@code paper-global.yml}.
     *
     * <p>Folia reads this file while the server starts, before {@code config/tuff.yml} exists as a
     * parsed tree, which is why the scheduler selection lives here and the opt-in lives there. The
     * function edits only the three keys it owns and leaves the rest of the generated file untouched;
     * if the block is missing it refuses instead of appending a second, ignored section.
     *
     * @param runDir  run directory
     * @param arm     arm to configure
     * @param threads tick thread count for Folia
     * @param gridExp region grid exponent
     * @throws IOException if the file cannot be read or written
     */
    static void patchPaperGlobal(final Path runDir, final Arm arm, final int threads, final int gridExp)
        throws IOException {
        final Path file = runDir.resolve("config").resolve("paper-global.yml");
        if (!Files.isRegularFile(file)) {
            throw new IOException("missing " + file + "; the template was not booted correctly");
        }
        final List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        final List<String> out = new ArrayList<>(lines.size());
        boolean inBlock = false;
        int blockIndent = -1;
        boolean patchedScheduler = false;
        boolean patchedThreads = false;
        boolean patchedGrid = false;
        for (final String line : lines) {
            final int indent = indentOf(line);
            final String trimmed = line.strip();
            if (trimmed.equals("threaded-regions:")) {
                inBlock = true;
                blockIndent = indent;
                out.add(line);
                continue;
            }
            if (inBlock && !trimmed.isEmpty() && indent <= blockIndent) {
                inBlock = false;
            }
            if (inBlock && !trimmed.isEmpty()) {
                if (trimmed.startsWith("scheduler:")) {
                    out.add(" ".repeat(indent) + "scheduler: " + arm.scheduler());
                    patchedScheduler = true;
                    continue;
                }
                if (trimmed.startsWith("threads:")) {
                    out.add(" ".repeat(indent) + "threads: " + threads);
                    patchedThreads = true;
                    continue;
                }
                if (trimmed.startsWith("grid-exponent:")) {
                    out.add(" ".repeat(indent) + "grid-exponent: " + gridExp);
                    patchedGrid = true;
                    continue;
                }
            }
            out.add(line);
        }
        if (!patchedScheduler || !patchedThreads || !patchedGrid) {
            throw new IOException(
                "paper-global.yml has no complete threaded-regions block: scheduler="
                    + patchedScheduler + " threads=" + patchedThreads + " grid=" + patchedGrid
            );
        }
        Files.write(file, out, StandardCharsets.UTF_8);
    }

    private static int indentOf(final String line) {
        int indent = 0;
        while (indent < line.length() && line.charAt(indent) == ' ') {
            ++indent;
        }
        return indent;
    }

    /**
     * Copies the template into a fresh run directory.
     *
     * <p>Large immutable trees (the extracted libraries, versions and caches) are hard linked when the
     * file system allows it, so a block of dozens of runs does not multiply the disk usage; everything
     * the server writes is a real copy, so one run cannot mutate another's world.
     *
     * @param template source directory
     * @param target   destination directory
     * @throws IOException if the copy fails for a file whose hard link was refused
     */
    static void copyTemplate(final Path template, final Path target) throws IOException {
        Files.createDirectories(target);
        try (var walk = Files.walk(template)) {
            for (final Path source : walk.toList()) {
                final Path relative = template.relativize(source);
                final Path destination = target.resolve(relative.toString());
                if (Files.isDirectory(source)) {
                    Files.createDirectories(destination);
                    continue;
                }
                if (!Files.isRegularFile(source)) {
                    continue;
                }
                final String first = relative.getName(0).toString();
                final boolean heavy = first.equals("libraries") || first.equals("versions") || first.equals("cache");
                if (heavy && !Files.exists(destination)) {
                    try {
                        Files.createLink(destination, source);
                        continue;
                    } catch (final FileAlreadyExistsException _) {
                        continue;
                    } catch (final IOException | UnsupportedOperationException _) {
                        // different volume or no hard link support: fall through to a real copy
                    }
                }
                Files.copy(source, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
