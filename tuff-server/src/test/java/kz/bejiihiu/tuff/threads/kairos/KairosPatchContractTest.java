package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.concurrentutil.scheduler.Scheduler;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the shape of the port and the size of the patch.
 *
 * <p>Kairos is only cheap while the facade stays a handful of branches: the EDF path must be the
 * untouched Folia line, Kairos must be reachable from exactly one switch case, and the class must
 * implement all eight abstract methods of {@link Scheduler}. The source checks read the applied
 * layer, because that is what this build compiles; without it they are skipped rather than red.
 */
@Timeout(120)
class KairosPatchContractTest {

    private static final String SCHEDULER_SOURCE =
        "tuff-server/src/minecraft/java/io/papermc/paper/threadedregions/TickRegionScheduler.java";

    @Test
    void theAbstractApiStaysAtEightMethodsAndKairosImplementsAllOfThem() {
        final long abstractMethods = Arrays.stream(Scheduler.class.getDeclaredMethods())
            .filter(method -> Modifier.isAbstract(method.getModifiers()))
            .count();

        assertEquals(8, abstractMethods, "the scheduler contract changed; the port must follow it");
        assertFalse(Modifier.isAbstract(KairosScheduler.class.getModifiers()));
        assertTrue(Scheduler.class.isAssignableFrom(KairosScheduler.class));
    }

    @Test
    void theFacadeOnlyAddsTheKairosBranches() throws IOException {
        final Path file = repoRoot().resolve(SCHEDULER_SOURCE);
        Assumptions.assumeTrue(Files.isRegularFile(file), "applied source not present: " + file);
        final String source = Files.readString(file, StandardCharsets.UTF_8);

        assertEquals(1, count(source, "\n        KAIROS\n"), "KAIROS belongs in the enum exactly once");
        assertEquals(1, count(source, "case KAIROS:"), "one switch case creates the scheduler");
        assertEquals(1, count(source, "new kz.bejiihiu.tuff.threads.kairos.KairosScheduler("),
            "the facade builds one Kairos instance");
        assertEquals(1, count(source, "kairosScheduler.start()"), "start delegates to the fixed fund");
        assertEquals(1, count(source, "kairosScheduler.setThreads(threads)"),
            "setThreads delegates to the fund, which applies the floor");
        assertEquals(1, count(source, "this.scheduler = new EDFSchedulerThreadPool(initialThreads, threadFactory);"),
            "the EDF line must stay byte for byte the one Folia had");
        assertEquals(1, count(source, "public final long scheduledStart()"),
            "the deadline accessor is the only added API on the tick handle");
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
                KairosPatchContractTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()
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
