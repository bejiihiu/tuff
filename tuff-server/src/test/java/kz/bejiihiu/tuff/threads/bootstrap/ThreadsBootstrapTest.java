package kz.bejiihiu.tuff.threads.bootstrap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.bootstrap.ComputeUtmiAdapter;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.bridge.RegionThreads;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import kz.bejiihiu.tuff.threads.kairos.KairosServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadsBootstrapTest {

    @TempDir
    Path directory;

    @Test
    void anEnabledConfigurationRegistersTheObserveOnlyPools() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: true
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();

        bootstrap.startWith(new ThreadsConfig(file));

        final ThreadsStatus status = bootstrap.currentStatus();
        assertEquals(ThreadsStatus.State.RUNNING, status.state());
        assertTrue(bootstrap.regionPoolRegistered(), "the region pool must be registered by the runtime");
        final ResourceSnapshot snapshot = status.snapshot();
        assertNotNull(snapshot, "a running layer must produce a snapshot");
        final PoolSnapshot region = snapshot.pool(PoolId.REGION_EXECUTION).orElseThrow();
        assertEquals(CoverageLevel.L1, region.coverage(), "the default is observation only");
        assertEquals(0, region.spec().minThreads());
        final PoolSnapshot service = snapshot.pool(PoolId.SERVICE).orElseThrow();
        assertEquals(CoverageLevel.L2, service.coverage(), "the refresher needs an L2 pool to exist in");
        assertTrue(service.aliveThreads() >= 1, "the refresher is accounted to the service pool");
        assertTrue(status.sampleAgeMillis() >= 0L, "the first pass runs before start returns");

        bootstrap.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, bootstrap.currentStatus().state());
        assertFalse(bootstrap.regionPoolRegistered());
    }

    @Test
    void aDisabledConfigurationCreatesNothingAtAll() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: false
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();

        bootstrap.startWith(new ThreadsConfig(file));

        final ThreadsStatus status = bootstrap.currentStatus();
        assertEquals(ThreadsStatus.State.DISABLED, status.state());
        assertNull(status.snapshot());
        assertFalse(bootstrap.regionPoolRegistered(), "a disabled layer must not register a pool");

        bootstrap.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, bootstrap.currentStatus().state());
    }

    @Test
    void aSecondStartIsANoOp() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: true
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        final ThreadsConfig config = new ThreadsConfig(file);

        bootstrap.startWith(config);
        bootstrap.startWith(config);

        assertEquals(ThreadsStatus.State.RUNNING, bootstrap.currentStatus().state());
        bootstrap.shutdown();
    }

    @Test
    void aLayerCanBeStartedAgainAfterItWasStopped() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: true
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        final ThreadsConfig config = new ThreadsConfig(file);

        bootstrap.startWith(config);
        assertEquals(ThreadsStatus.State.RUNNING, bootstrap.currentStatus().state());
        assertNotNull(bootstrap.adapterFor(ComputeSettings.defaults()), "the first start serves the engine");

        bootstrap.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, bootstrap.currentStatus().state());
        assertFalse(bootstrap.regionPoolRegistered(), "a stopped layer must not keep its pools");
        assertNull(bootstrap.adapterFor(ComputeSettings.defaults()), "a stopped layer has no adapter");

        bootstrap.startWith(config);

        assertEquals(ThreadsStatus.State.RUNNING, bootstrap.currentStatus().state(),
            "a start after stop must build a new runtime instead of quietly doing nothing");
        assertTrue(bootstrap.regionPoolRegistered(), "the second start registers the pools again");
        assertNotNull(bootstrap.adapterFor(ComputeSettings.defaults()), "the second start serves the engine again");
        bootstrap.shutdown();
        assertEquals(ThreadsStatus.State.NOT_INITIALISED, bootstrap.currentStatus().state());
    }

    @Test
    void aMissingConfigurationStillStartsWithTheDefaults() {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();

        bootstrap.startWith(new ThreadsConfig(this.directory.resolve("absent.yml")));

        assertEquals(ThreadsStatus.State.RUNNING, bootstrap.currentStatus().state());
        assertTrue(bootstrap.regionPoolRegistered());
        bootstrap.shutdown();
    }

    @Test
    void aManagedRegionPoolIsRegisteredAtL2BeforeTheSchedulerExists() throws IOException {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
              region-execution:
                managed: true
            """)));

        final PoolSnapshot region = bootstrap.currentStatus().snapshot()
            .pool(PoolId.REGION_EXECUTION).orElseThrow();
        assertEquals(CoverageLevel.L2, region.coverage(), "managed means L2, not L3");
        assertEquals(1, region.spec().minThreads(), "an L2 pool needs a positive floor");

        final Runnable body = () -> { };
        assertNotSame(body, RegionThreads.wrapBody(body),
            "a managed region pool must wrap the bodies Folia's factory hands over");

        bootstrap.shutdown();
        assertSame(body, RegionThreads.wrapBody(body),
            "after stop nothing may be wrapped, including bodies created during shutdown");
    }

    @Test
    void anObserveOnlyRegionPoolKeepsFoliaThreadsUntouched() throws IOException {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
            """)));

        final PoolSnapshot region = bootstrap.currentStatus().snapshot()
            .pool(PoolId.REGION_EXECUTION).orElseThrow();
        assertEquals(CoverageLevel.L1, region.coverage());
        assertEquals(0, region.spec().minThreads());

        final Runnable body = () -> { };
        assertSame(body, RegionThreads.wrapBody(body), "the default configuration must change nothing");

        bootstrap.shutdown();
    }

    @Test
    void theComputeAdapterFollowsTheManagedFlag() throws IOException {
        final ThreadsBootstrap managed = new ThreadsBootstrap();
        managed.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
              execution-pools:
                compute:
                  managed: true
            """)));

        final ComputeUtmiAdapter adapter = managed.adapterFor(ComputeSettings.defaults());
        assertNotNull(adapter, "a running layer must serve the engine");
        assertTrue(adapter.isManaged());
        assertEquals("utmi", adapter.source());
        // the command reads the cached sample, so a registration made after the last pass becomes
        // visible on the next one (at most a second later)
        assertEquals(CoverageLevel.L4, awaitCoverage(managed, PoolId.COMPUTE, CoverageLevel.L4));
        final PoolSnapshot compute = managed.currentStatus().snapshot().pool(PoolId.COMPUTE).orElseThrow();
        assertEquals(ComputeSettings.defaults().workers().minThreads(), compute.spec().minThreads());
        managed.shutdown();
    }

    @Test
    void anObserveOnlyLayerStillServesTheAdapterButNotThePorts() throws IOException {
        final ThreadsBootstrap observe = new ThreadsBootstrap();
        observe.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
            """)));

        final ComputeUtmiAdapter adapter = observe.adapterFor(ComputeSettings.defaults());
        assertNotNull(adapter, "the pool is observed, which needs the probe");
        assertFalse(adapter.isManaged());
        final PoolSnapshot compute = observe.currentStatus().snapshot().pool(PoolId.COMPUTE).orElseThrow();
        assertEquals(CoverageLevel.L1, compute.coverage(), "no flag means the engine keeps its own threads");
        observe.shutdown();
    }

    /**
     * Waits for the cached sample to show a pool at the expected coverage.
     *
     * <p>The command path never samples, so a registration done after the last pass is visible only on
     * the next one. Polling is the honest way for a test to wait for that instead of reaching into the
     * refresher.
     *
     * @param bootstrap running layer
     * @param pool      pool to watch
     * @param expected  coverage the test is waiting for
     * @return the last observed coverage
     */
    private static CoverageLevel awaitCoverage(
        final ThreadsBootstrap bootstrap,
        final PoolId pool,
        final CoverageLevel expected
    ) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        CoverageLevel seen = null;
        while (System.nanoTime() < deadline) {
            final ResourceSnapshot snapshot = bootstrap.currentStatus().snapshot();
            if (snapshot != null) {
                final PoolSnapshot current = snapshot.pool(pool).orElse(null);
                if (current != null) {
                    seen = current.coverage();
                    if (seen == expected) {
                        return seen;
                    }
                }
            }
            try {
                Thread.sleep(20L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return seen;
            }
        }
        return seen;
    }

    @Test
    void aDisabledLayerHasNoComputeAdapter() throws IOException {
        final ThreadsBootstrap disabled = new ThreadsBootstrap();
        disabled.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: false
            """)));

        assertNull(disabled.adapterFor(ComputeSettings.defaults()));
        disabled.shutdown();
    }

    @Test
    void threadsListedByAProbeAreNotReportedAsUnknown() {
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 1, 0, 1, 1, 0, 1, 0.0d, 0.0d, -1.0d,
            List.of(),
            List.of(new ThreadSnapshot(7L, "Folia Region Scheduler Thread #1", PoolId.REGION_EXECUTION,
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L)),
            Set.of()
        );
        final List<ThreadSnapshot> unregistered = List.of(
            new ThreadSnapshot(7L, "Folia Region Scheduler Thread #1", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(9L, "Netty Epoll Server IO #0", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L)
        );

        assertEquals(1, ThreadsSampler.countUnknown(unregistered, snapshot));
    }

    @Test
    void everyObservedPoolIsRegisteredWithItsOwnCoverage() throws IOException {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
            """)));
        try {
            final ResourceSnapshot snapshot = bootstrap.currentStatus().snapshot();
            assertNotNull(snapshot, "a running layer must publish the canonical pool set");
            final Set<PoolId> ids = new HashSet<>();
            for (final PoolSnapshot pool : snapshot.pools()) {
                ids.add(pool.id());
            }
            assertEquals(Set.of(
                PoolId.REGION_EXECUTION,
                PoolId.COMPUTE,
                PoolId.COMPUTE_VIRTUAL,
                PoolId.CHUNK_WORKER,
                PoolId.CHUNK_IO,
                PoolId.NETWORK,
                PoolId.CHAT,
                PoolId.BACKGROUND,
                PoolId.SERVICE
            ), ids, "every observed pool has a spec, so the command can print it");
            for (final PoolId id : Set.of(
                PoolId.CHUNK_WORKER, PoolId.CHUNK_IO, PoolId.NETWORK, PoolId.CHAT, PoolId.BACKGROUND
            )) {
                final PoolSnapshot pool = snapshot.pool(id).orElseThrow();
                assertEquals(CoverageLevel.L1, pool.coverage(), id + " is observed only");
                assertEquals(0, pool.spec().minThreads(), id + " claims no managed floor");
            }
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void everyObservedPoolAccountsForItsOwnThreadsInTheUnknownCount() {
        final List<ThreadSnapshot> observed = List.of(
            new ThreadSnapshot(11L, "Netty NIO IO #0", PoolId.NETWORK,
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(12L, "Tuff Common Worker #0", PoolId.CHUNK_WORKER,
                Thread.State.RUNNABLE, true, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(13L, "Tuff I/O Worker #0", PoolId.CHUNK_IO,
                Thread.State.WAITING, true, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(14L, "Async Chat Thread - #0", PoolId.CHAT,
                Thread.State.RUNNABLE, true, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(15L, "Worker-Main-1", PoolId.BACKGROUND,
                Thread.State.TIMED_WAITING, true, Thread.NORM_PRIORITY, -1L)
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 1, 0, 1, 1, 0, 1, 0.0d, 0.0d, -1.0d,
            List.of(),
            observed,
            Set.of()
        );
        final List<ThreadSnapshot> unregistered = List.of(
            new ThreadSnapshot(11L, "Netty NIO IO #0", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, false, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(12L, "Tuff Common Worker #0", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, true, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(13L, "Tuff I/O Worker #0", PoolId.of("unregistered"),
                Thread.State.WAITING, true, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(14L, "Async Chat Thread - #0", PoolId.of("unregistered"),
                Thread.State.RUNNABLE, true, Thread.NORM_PRIORITY, -1L),
            new ThreadSnapshot(15L, "Worker-Main-1", PoolId.of("unregistered"),
                Thread.State.TIMED_WAITING, true, Thread.NORM_PRIORITY, -1L)
        );

        assertEquals(0, ThreadsSampler.countUnknown(unregistered, snapshot),
            "a thread listed by any probe is accounted, not unknown");
    }

    @Test
    void theKairosSeamsComeFromTheControllerAndTheServicePool() throws IOException, InterruptedException {
        final Path file = this.write("""
            thread-management:
              enabled: true
              region-execution:
                threads: 2
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(file));

        final KairosServices services = bootstrap.kairosServices();
        assertNotNull(services.budget(), "a running layer gives Kairos a budget port");
        final int usable = bootstrap.currentStatus().snapshot().usableCores();
        assertEquals(
            Math.max(2, usable + 2),
            services.budget().ceiling(2),
            "the ceiling is the controller's usable cores plus the region threads it already deducted"
        );

        final Thread supervisor = services.supervisors().create(
            "Tuff Kairos Supervisor", () -> { }, (thread, failure) -> { }
        );
        assertNotNull(supervisor, "the service pool is L2, so the provider must accept the thread");
        assertTrue(supervisor.getName().startsWith("Tuff Kairos Supervisor"), supervisor.getName());
        supervisor.start();
        supervisor.join(TimeUnit.SECONDS.toMillis(5L));
        assertFalse(supervisor.isAlive());

        bootstrap.shutdown();
    }

    @Test
    void aDisabledLayerHasNoKairosSeams() throws IOException {
        final Path file = this.write("""
            thread-management:
              enabled: false
            """);
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(file));

        final KairosServices services = bootstrap.kairosServices();
        assertNull(services.budget(), "without a runtime the fund falls back to the core count");
        assertNull(
            services.supervisors().create("Tuff Kairos Supervisor", () -> { }, (thread, failure) -> { }),
            "without a runtime the scheduler builds its own daemon"
        );

        bootstrap.shutdown();
    }

    private Path write(final String contents) throws IOException {
        final Path file = this.directory.resolve("tuff.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        return file;
    }
}
