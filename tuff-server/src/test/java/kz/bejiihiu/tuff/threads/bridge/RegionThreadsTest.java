package kz.bejiihiu.tuff.threads.bridge;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the region bridge is a pass-through unless the pool is really at L2.
 *
 * <p>Folia's factory must always get a runnable it can run. Every failure mode of the thread layer -
 * not started, disabled, observe-only, stopped provider - therefore has to end with the original body,
 * not with an exception and not with a half wrapped thread. The one case that does wrap must keep the
 * thread's identity intact: same name, same body, accounting only for the duration of the body.
 */
@Timeout(120)
class RegionThreadsTest {

    private static final String REGION_NAME = "Folia Region Scheduler Thread #0";

    private static final String FACTORY =
        "tuff-server/src/minecraft/java/io/papermc/paper/threadedregions/TickRegionScheduler.java";

    @AfterEach
    void clearTheBridge() {
        RegionThreads.clear();
    }

    @Test
    void withoutABindingTheSameBodyComesBack() {
        RegionThreads.clear();
        final Runnable body = () -> { };

        assertSame(body, RegionThreads.wrapBody(body), "an idle bridge must hand back the exact instance");
        assertFalse(RegionThreads.installed());
    }

    @Test
    void anObserveOnlyLayerLeavesRegionBodiesAlone() {
        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory()
            .create(ThreadManagementSettings.defaults());
        try {
            runtime.start();
            final UnifiedThreadManager manager = runtime.manager();
            assertEquals(CoverageLevel.L1, region(manager).coverage(), "the default configuration observes only");
            RegionThreads.install(manager);

            final Runnable body = () -> { };
            assertSame(body, RegionThreads.wrapBody(body), "an L1 pool must not be wrapped");
            assertFalse(RegionThreads.installed());
        } finally {
            runtime.shutdown(Duration.ZERO);
        }
    }

    @Test
    void aManagedRegionPoolWrapsAndAccountsTheBody() throws InterruptedException {
        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory().create(managedRegion());
        try {
            runtime.start();
            final UnifiedThreadManager manager = runtime.manager();
            final PoolSpec spec = region(manager);
            assertEquals(CoverageLevel.L2, spec.coverage());
            assertEquals(1, spec.minThreads(), "L2 needs a positive floor");

            RegionThreads.install(manager);
            assertTrue(RegionThreads.installed());

            final Runnable body = () -> { };
            assertNotSame(body, RegionThreads.wrapBody(body), "an L2 pool must be wrapped");

            final AtomicReference<Optional<PoolId>> accountedWhileRunning = new AtomicReference<>();
            final AtomicReference<String> nameWhileRunning = new AtomicReference<>();
            final Thread tick = new Thread(
                RegionThreads.wrapBody(() -> {
                    accountedWhileRunning.set(manager.poolOf(Thread.currentThread()));
                    nameWhileRunning.set(Thread.currentThread().getName());
                }),
                REGION_NAME
            );
            tick.start();
            tick.join();

            assertEquals(Optional.of(PoolId.REGION_EXECUTION), accountedWhileRunning.get(),
                "the thread must be accounted for the duration of its body");
            assertEquals(REGION_NAME, nameWhileRunning.get(), "the bridge must not touch the thread name");
            assertEquals(REGION_NAME, tick.getName());
            assertTrue(manager.poolOf(tick).isEmpty(), "the thread must be released when the body returns");
        } finally {
            runtime.shutdown(Duration.ZERO);
        }
    }

    @Test
    void theWrapperDoesNotSwallowFailuresFromTheBody() {
        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory().create(managedRegion());
        try {
            runtime.start();
            RegionThreads.install(runtime.manager());

            final Runnable wrapped = RegionThreads.wrapBody(() -> {
                throw new IllegalStateException("tick blew up");
            });

            final IllegalStateException failure = assertThrows(IllegalStateException.class, wrapped::run);
            assertEquals("tick blew up", failure.getMessage(),
                "a failure in the body must reach Folia's uncaught handler untouched");
            assertTrue(runtime.manager().poolOf(Thread.currentThread()).isEmpty(),
                "the wrapper must release the thread even when the body throws");
        } finally {
            runtime.shutdown(Duration.ZERO);
        }
    }

    @Test
    void aProviderThatRefusesFallsBackToTheOriginalBody() {
        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory().create(managedRegion());
        runtime.start();
        RegionThreads.install(runtime.manager());
        runtime.shutdown(Duration.ZERO);

        final Runnable body = () -> { };
        assertSame(body, RegionThreads.wrapBody(body),
            "a stopped provider must not cost the scheduler its threads");
    }

    @Test
    void theRegionFactoryWrapsTheBodyAndKeepsEverythingElse() throws IOException {
        final Path file = repoRoot().resolve(FACTORY);
        Assumptions.assumeTrue(Files.isRegularFile(file), "applied source not present: " + file);
        final String source = Files.readString(file, StandardCharsets.UTF_8);

        assertEquals(1, count(source, "RegionThreads.wrapBody(run)"),
            "exactly one call belongs in the region thread factory");
        assertEquals(1, count(source, "new TickThreadRunner(this.threadGroup,"),
            "the thread class and its group must stay Folia's");
        assertEquals(1, count(source, "Folia Region Scheduler Thread #"),
            "the thread name is an operational contract and must not change");
        assertEquals(1, count(source, "ret.setUncaughtExceptionHandler(TickRegionScheduler.this::uncaughtException)"),
            "the original handler must stay the only one");
    }

    private static PoolSpec region(final UnifiedThreadManager manager) {
        return manager.pools().spec(PoolId.REGION_EXECUTION).orElseThrow(
            () -> new IllegalStateException("the region pool must be registered")
        );
    }

    private static ThreadManagementSettings managedRegion() {
        final ThreadManagementSettings base = ThreadManagementSettings.defaults();
        final ThreadManagementSettings.RegionExecution region = new ThreadManagementSettings.RegionExecution(
            true, true, ThreadManagementSettings.RegionScheduler.EDF, -1, 4, false
        );
        return new ThreadManagementSettings(
            base.enabled(),
            base.resources(),
            base.lifecycle(),
            region,
            base.asyncCompute(),
            base.executionPools(),
            base.metrics()
        );
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
                RegionThreadsTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()
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
