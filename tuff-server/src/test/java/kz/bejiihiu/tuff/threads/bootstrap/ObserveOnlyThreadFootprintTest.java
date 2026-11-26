package kz.bejiihiu.tuff.threads.bootstrap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.observe.TuffErrorLogCapture;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the observe-only layer creates exactly one thread and takes it away again.
 *
 * <p>Observe-only is a promise about side effects, not about code paths: the layer may read the
 * thread table, register pool descriptions and build snapshots. The one exception is the background
 * refresher, because Folia runs console commands on a region tick thread and the heavy reads have to
 * live somewhere else. So the contract tested here is: starting the layer creates the sampler and
 * nothing else, the sampler is accounted to the service pool, and stopping the layer leaves no thread
 * behind - neither a leak nor a half stopped loop.
 *
 * <p>Threads are compared as a delta (id to name) of {@link Thread#getAllStackTraces()}, filtered to
 * names this project owns. A delta instead of an absolute set, because the test JVM is shared with the
 * test runner and with other tests of the same suite, and those bring their own threads.
 */
@Timeout(180)
class ObserveOnlyThreadFootprintTest {

    /**
     * Prefixes and suffixes that mark a thread as ours. Compute names its workers
     * {@code Tuff Compute Worker-1}, the service threads {@code <prefix>-janitor} and the lane threads
     * {@code <prefix>-virtual-pump} / {@code <prefix>-completion-pool-1}.
     */
    private static final List<String> OWN_PREFIXES = List.of("Tuff", "tuff-threads");
    private static final List<String> OWN_INFIXES = List.of("-janitor", "-virtual", "-completion");

    @TempDir
    Path directory;

    private @Nullable ThreadsBootstrap layer;

    @AfterEach
    void stopWhateverStarted() {
        if (this.layer != null) {
            this.layer.shutdown();
            this.layer = null;
        }
    }

    @Test
    void observeOnlyAddsOnlyTheRefresherAndRemovesItOnStop() throws IOException {
        final SortedMap<Long, String> before = ourThreads();

        this.layer = this.start("""
            thread-management:
              enabled: true
            """);

        final ThreadsStatus running = this.layer.currentStatus();
        assertEquals(ThreadsStatus.State.RUNNING, running.state());
        assertTrue(this.layer.regionPoolRegistered(), "observe-only still registers the region pool");
        final ResourceSnapshot snapshot = running.snapshot();
        assertNotNull(snapshot, "a running layer publishes the first sample before start returns");
        assertEquals(7, snapshot.pools().size(), "the canonical pool set is described, not created");

        final Map<Long, String> added = newOurThreads(before);
        assertEquals(1, added.size(), "the refresher is the only thread the layer creates: " + added);
        assertTrue(added.values().iterator().next().startsWith("Tuff Threads Sampler"), added.toString());
        assertEquals(1, snapshot.pool(PoolId.SERVICE).orElseThrow().aliveThreads(),
            "the refresher is accounted to the service pool");

        this.layer.shutdown();

        assertTrue(waitForNoSampler(5_000L), "stopping the layer must take the refresher down");
        assertEquals(Map.of(), newOurThreads(before), "stopping the layer must not leave a thread behind");
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, this.layer.currentStatus().state());
        assertFalse(this.layer.regionPoolRegistered());
    }

    @Test
    void aDisabledLayerCreatesNoRuntimeAndNoThread() throws IOException {
        final SortedMap<Long, String> before = ourThreads();

        this.layer = this.start("""
            thread-management:
              enabled: false
            """);

        final ThreadsStatus status = this.layer.currentStatus();
        assertEquals(ThreadsStatus.State.DISABLED, status.state());
        assertEquals(-1, status.unregisteredThreads(), "a disabled layer measured nothing");
        assertFalse(status.metricsEnabled());
        assertFalse(this.layer.regionPoolRegistered(), "no runtime means no registered pool");
        assertEquals(Map.of(), newOurThreads(before), "the kill switch must not create a thread");

        this.layer.shutdown();
        assertEquals(Map.of(), newOurThreads(before), "shutting a disabled layer down changes nothing");
    }

    @Test
    void startingTwiceRegistersTheSamePoolsOnceAndKeepsOneRefresher() throws IOException {
        final SortedMap<Long, String> before = ourThreads();
        this.layer = new ThreadsBootstrap();
        final ThreadsConfig config = new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
            """));

        this.layer.startWith(config);
        this.layer.startWith(config);
        this.layer.startWith(config);

        final ThreadsStatus status = this.layer.currentStatus();
        assertEquals(ThreadsStatus.State.RUNNING, status.state());
        assertEquals(7, status.snapshot().pools().size(), "a repeated start must not register a second pool set");
        assertEquals(1, newOurThreads(before).size(), "a repeated start must not spawn a second refresher");

        this.layer.shutdown();
        this.layer.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, this.layer.currentStatus().state());
        assertTrue(waitForNoSampler(5_000L), "the first shutdown must stop the refresher");
        assertEquals(Map.of(), newOurThreads(before));

        // the instance is reusable, but nothing stale comes back: start after stop builds a fresh
        // runtime from the file, and the old one is already gone (no pool, no adapter, no thread)
        this.layer.startWith(config);
        assertEquals(ThreadsStatus.State.RUNNING, this.layer.currentStatus().state(),
            "a stopped layer must start again instead of silently ignoring the request");
        assertTrue(this.layer.regionPoolRegistered(), "the fresh runtime registers the canonical pools");
        assertEquals(7, this.layer.currentStatus().snapshot().pools().size());
        assertEquals(1, newOurThreads(before).size(), "the restart brings exactly one refresh thread back");

        this.layer.shutdown();
        assertTrue(waitForNoSampler(5_000L), "the second shutdown must stop the refresher too");
        assertEquals(Map.of(), newOurThreads(before), "restarting must not leave a thread behind either");
    }

    @Test
    void aStoppedRuntimeRefusesPoolsLeasesAndThreadCreation() throws InterruptedException {
        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory()
            .create(ThreadManagementSettings.defaults());
        final UnifiedThreadManager manager = runtime.manager();

        runtime.start();
        runtime.start();
        assertEquals(UnifiedThreadManager.State.RUNNING, manager.state());
        assertEquals(7, manager.pools().ids().size(), "a second start must not add pools");

        runtime.shutdown(Duration.ZERO);
        runtime.shutdown(Duration.ZERO);
        assertTrue(runtime.awaitTermination(Duration.ofSeconds(10)));
        assertEquals(UnifiedThreadManager.State.STOPPED, manager.state());

        assertThrows(
            IllegalStateException.class,
            () -> manager.pools().register(PoolSpec.observeOnly(PoolId.SERVICE, PoolKind.SERVICE, "test", "late")),
            "a stopped layer must not accept a pool"
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.threadFactory().newThread(
                ThreadSpec.of(PoolId.COMPUTE, "late-thread", (thread, failure) -> { }),
                () -> { }
            ),
            "a stopped layer must not create a thread"
        );
        assertThrows(
            IllegalStateException.class,
            () -> manager.resources().acquire(
                LeaseRequest.immediate(PoolId.COMPUTE, System.nanoTime() + 1_000_000_000L)
            ),
            "a stopped layer must not hand out a lease"
        );
        assertThrows(IllegalStateException.class, runtime::start, "a stopped runtime does not restart");
    }

    @Test
    void aLayerWithMetricsOffReportsDisabledAndPrintsNoSentinel() throws IOException {
        this.layer = this.start("""
            thread-management:
              enabled: true
              metrics:
                enabled: false
            """);

        final ThreadsStatus status = this.layer.currentStatus();
        assertEquals(ThreadsStatus.State.RUNNING, status.state());
        assertFalse(status.metricsEnabled());
        assertEquals(-1, status.unregisteredThreads(),
            "the unaccounted report must be skipped, not measured, when collection is off");
        assertNotNull(status.snapshot());

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains("metrics: disabled"), text);
        assertTrue(text.contains("unknown threads: n/a"), text);
        assertTrue(text.contains("region-execution"), "the registered pools are still listed: " + text);
        assertFalse(text.contains("-1"), "a sentinel must never be printed as a number:\n" + text);
        assertFalse(text.contains("unknown -1"), text);
    }

    @Test
    void observeOnlyLogsNoError() throws IOException {
        final @Nullable TuffErrorLogCapture capture = TuffErrorLogCapture.attach();
        Assumptions.assumeTrue(capture != null, "no log4j2 core on the test classpath");
        try (capture) {
            this.layer = this.start("""
                thread-management:
                  enabled: true
                """);
            this.layer.currentStatus();

            assertTrue(capture.errors().isEmpty(), () -> "observe-only logged an error: " + capture.errors());

            this.layer.shutdown();
            assertTrue(capture.errors().isEmpty(), () -> "stopping the layer logged an error: " + capture.errors());
        }
    }

    private ThreadsBootstrap start(final String yaml) throws IOException {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(this.write(yaml)));
        return bootstrap;
    }

    private Path write(final String contents) throws IOException {
        final Path file = this.directory.resolve("tuff.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        return file;
    }

    /**
     * Snapshot of every live thread this project owns.
     *
     * @return thread id to name, sorted by id
     */
    private static SortedMap<Long, String> ourThreads() {
        final SortedMap<Long, String> found = new TreeMap<>();
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            final String name = thread.getName();
            if (isOurs(name)) {
                found.put(thread.threadId(), name);
            }
        }
        return found;
    }

    /**
     * Threads that appeared after {@code before} and belong to this project.
     *
     * @param before snapshot taken before the action
     * @return id to name of the new threads, empty when nothing was created
     */
    private static Map<Long, String> newOurThreads(final Map<Long, String> before) {
        final SortedMap<Long, String> created = new TreeMap<>(ourThreads());
        created.keySet().removeAll(before.keySet());
        return created;
    }

    private static boolean isOurs(final String name) {
        for (final String prefix : OWN_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        for (final String marker : OWN_INFIXES) {
            if (name.contains(marker)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Waits until no refresher thread is alive.
     *
     * <p>The shutdown path never joins, so the test is the one that has to wait. Polling is fine here:
     * the interrupt ends the sleep immediately and the loop exits on the next check.
     *
     * @param millis how long to wait
     * @return {@code true} when the refresher is gone
     */
    private static boolean waitForNoSampler(final long millis) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            if (samplerThreads().isEmpty()) {
                return true;
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return samplerThreads().isEmpty();
            }
        }
        return samplerThreads().isEmpty();
    }

    private static List<Thread> samplerThreads() {
        final List<Thread> found = new ArrayList<>();
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith("Tuff Threads Sampler")) {
                found.add(thread);
            }
        }
        return found;
    }
}
