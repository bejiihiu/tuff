package kz.bejiihiu.tuff.threads.observe;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the patch budget of the observe-only step: zero lines in files we do not own.
 *
 * <p>Observe-only rides on hooks that already exist for the compute engine. CraftServer already starts
 * the engine, MinecraftServer already stops it and already applies the region budget after the region
 * threads exist, and PaperCommands already registers {@code /tuff}. The step is only "free" while that
 * stays true: a new {@code ThreadsBootstrap} call in a server file would be a new patch, a new conflict
 * surface on every upstream merge, and it would mean the layer started reaching into Folia.
 *
 * <p>The test reads the applied sources, not the patch files: what matters is what is compiled for this
 * build. When the sources are absent (a checkout without the applied patch layer), the checks are
 * skipped instead of red - a guard that only works on one machine is worse than a documented skip.
 */
@Timeout(120)
class AppliedHookInvariantTest {

    private static final String CRAFT_SERVER = "paper-server/src/main/java/org/bukkit/craftbukkit/CraftServer.java";
    private static final String MINECRAFT_SERVER = "tuff-server/src/minecraft/java/net/minecraft/server/MinecraftServer.java";
    private static final String PAPER_COMMANDS = "paper-server/src/main/java/io/papermc/paper/command/PaperCommands.java";

    /**
     * The only Tuff call sites the three patched files may contain.
     */
    private static final int EXPECTED_TUFF_LINES = 4;

    @Test
    void theExistingComputeHooksStayTheOnlyOnes() throws IOException {
        final Path root = repoRoot();
        final Path craftServer = existing(root, CRAFT_SERVER);
        final Path minecraftServer = existing(root, MINECRAFT_SERVER);
        final Path paperCommands = existing(root, PAPER_COMMANDS);

        final String craft = read(craftServer);
        final String minecraft = read(minecraftServer);
        final String commands = read(paperCommands);

        assertEquals(1, count(craft, "ComputeBootstrap.start()"), "one startup hook in CraftServer");
        assertEquals(1, count(minecraft, "ComputeBootstrap.stop()"), "one stop hook in MinecraftServer");
        assertEquals(1, count(minecraft, "ComputeBootstrap.applyRegionBudget()"), "one budget hook in MinecraftServer");
        assertEquals(1, count(commands, "TuffCommand.create()"), "one command registration in PaperCommands");

        assertEquals(
            EXPECTED_TUFF_LINES,
            count(craft, "kz.bejiihiu.tuff")
                + count(minecraft, "kz.bejiihiu.tuff")
                + count(commands, "kz.bejiihiu.tuff"),
            "no new Tuff hook may appear in a server file the project patches"
        );
    }

    @Test
    void observeOnlyStillNeedsNoLineInAnyServerFile() throws IOException {
        final Path root = repoRoot();
        final List<Path> applied = List.of(
            existing(root, CRAFT_SERVER),
            existing(root, MINECRAFT_SERVER),
            existing(root, PAPER_COMMANDS)
        );

        final List<String> violations = new ArrayList<>();
        for (final Path file : applied) {
            final String source = read(file);
            for (final String forbidden : List.of("ThreadsBootstrap", "tuff.threads", "RegionThreads")) {
                if (source.contains(forbidden)) {
                    violations.add(root.relativize(file).toString().replace('\\', '/') + ": " + forbidden);
                }
            }
        }

        assertTrue(
            violations.isEmpty(),
            () -> "observe-only must keep zero lines in files the project patches, found:\n"
                + String.join("\n", violations)
        );
    }

    private static Path existing(final Path root, final String relative) {
        final Path file = root.resolve(relative);
        Assumptions.assumeTrue(Files.isRegularFile(file), "applied source not present: " + file);
        return file;
    }

    private static String read(final Path file) throws IOException {
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    private static int count(final String haystack, final String needle) {
        int found = 0;
        int from = 0;
        while ((from = haystack.indexOf(needle, from)) >= 0) {
            found++;
            from += needle.length();
        }
        return found;
    }

    /**
     * Finds the repository root without trusting the working directory.
     *
     * <p>The server test task runs in a Gradle temp directory, so {@code user.dir} is useless here.
     * The class file location is not: it lives under {@code <repo>/tuff-server/build/classes/...}, and
     * the first ancestor with {@code settings.gradle.kts} is the root. The class path is the fallback
     * when the classes are loaded from a jar.
     *
     * @return the repository root
     * @throws IllegalStateException if no ancestor looks like the repository root
     */
    private static Path repoRoot() {
        final Path fromClasses = fromClassLocation();
        if (fromClasses != null) {
            return fromClasses;
        }
        for (final String entry : System.getProperty("java.class.path", "").split(File.pathSeparator)) {
            if (entry.isBlank()) {
                continue;
            }
            final Path found = findRoot(Paths.get(entry).toAbsolutePath());
            if (found != null) {
                return found;
            }
        }
        throw new IllegalStateException("cannot find the repository root from the class path");
    }

    private static Path fromClassLocation() {
        try {
            final Path location = Paths.get(
                AppliedHookInvariantTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()
            );
            return findRoot(location);
        } catch (URISyntaxException | RuntimeException _) {
            return null;
        }
    }

    private static Path findRoot(final Path start) {
        Path candidate = start;
        for (int lookup = 0; lookup < 10 && candidate != null; lookup++) {
            if (Files.isRegularFile(candidate.resolve("settings.gradle.kts"))
                && Files.isDirectory(candidate.resolve("tuff-server"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        return null;
    }
}
