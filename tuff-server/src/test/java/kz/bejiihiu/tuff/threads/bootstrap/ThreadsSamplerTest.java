package kz.bejiihiu.tuff.threads.bootstrap;

import ca.spottedleaf.moonrise.common.util.TickThread;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import kz.bejiihiu.tuff.threads.internal.runtime.DefaultThreadManagerFactory;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The refresher is the reason the console can see anything on Folia.
 *
 * <p>Two things are tested here that the old command path could not do: a region read and a thread
 * table walk happening off the tick thread, and a command on a tick thread answering from the cache.
 * Nothing here needs a server: the region part uses a probe that reports a fixed summary, which is
 * exactly the shape the Folia probe produces.
 */
@Timeout(120)
class ThreadsSamplerTest {

    @TempDir
    Path directory;

    @Test
    void theRefresherIsADaemonThreadOfTheServicePool() {
        final ThreadManagerRuntime runtime = this.runtime();
        final ThreadsSampler sampler = ThreadsSampler.start(runtime);
        try {
            final Thread thread = samplerThread();
            assertNotNull(thread, "the factory must create the refresher");
            assertTrue(thread.isDaemon(), "a stopped server must not be kept alive by the refresher");
            assertTrue(thread.getName().startsWith("Tuff Threads Sampler"), thread.getName());
            assertEquals(PoolId.SERVICE, runtime.manager().poolOf(thread).orElseThrow(),
                "an unregistered refresher would show up as an unknown thread");

            final PoolSnapshot service = runtime.manager().snapshot().pool(PoolId.SERVICE).orElseThrow();
            assertEquals(CoverageLevel.L2, service.coverage(),
                "the service pool is raised to L2 so the provider can create the thread");
            assertEquals(1, service.spec().minThreads());
        } finally {
            sampler.stop();
            stop(runtime);
        }
    }

    @Test
    void theFirstPassIsAlreadyInTheCache() {
        final ThreadManagerRuntime runtime = this.runtime();
        final ThreadsSampler sampler = ThreadsSampler.start(runtime);
        try {
            final ThreadsSampler.Sample sample = sampler.sample();
            assertNotNull(sample, "start warms the cache so the first command is not empty");
            assertEquals(7, sample.snapshot().pools().size(), "the canonical pool set is measured");
            assertTrue(sample.unknownThreads() >= 0, "the refresher measures the unaccounted threads");
            assertTrue(sample.ageMillis() < 60_000L, "the first sample is fresh");
        } finally {
            sampler.stop();
            stop(runtime);
        }
    }

    @Test
    void regionMetricsFromAProbeLandInTheCachedSample() {
        final ThreadManagerRuntime runtime = this.runtime();
        runtime.attachProbe(new FixedRegionProbe());
        final ThreadsSampler sampler = ThreadsSampler.start(runtime);
        try {
            final ResourceSnapshot snapshot = sampler.sample().snapshot();
            final RegionPoolMetrics regions = snapshot.pool(PoolId.REGION_EXECUTION).orElseThrow()
                .regionMetrics().orElseThrow();
            assertEquals(3, regions.regions(), "the probe's reading is what the command later shows");
            assertEquals(19.5d, regions.minimumTps());
        } finally {
            sampler.stop();
            stop(runtime);
        }
    }

    @Test
    void anUnknownThreadIsCountedByTheRefresher() throws InterruptedException {
        final Thread stray = new Thread(() -> {
            try {
                Thread.sleep(60_000L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }, "tuff-sampler-unknown-probe");
        stray.setDaemon(true);
        stray.start();
        final ThreadManagerRuntime runtime = this.runtime();
        final ThreadsSampler sampler = ThreadsSampler.start(runtime);
        try {
            assertTrue(sampler.sample().unknownThreads() >= 1,
                "a thread nobody registered must be visible in the cached count");
        } finally {
            sampler.stop();
            stop(runtime);
            stray.interrupt();
        }
    }

    @Test
    void aCommandOnATickThreadAnswersFromTheCache() throws IOException, InterruptedException {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: true
            """)));
        try {
            final ThreadsStatus warm = bootstrap.currentStatus();
            assertNotNull(warm.snapshot(), "the startup path takes the first pass");

            final AtomicReference<ThreadsStatus> onTickThread = new AtomicReference<>();
            final Thread tick = new TickThread(() -> onTickThread.set(bootstrap.currentStatus()),
                "test-console-thread");
            tick.start();
            tick.join(5_000L);

            final ThreadsStatus status = onTickThread.get();
            assertNotNull(status, "the command must answer on a tick thread");
            assertNotNull(status.snapshot(), "the cached snapshot is what a tick thread reads");
            assertTrue(status.sampleAgeMillis() >= warm.sampleAgeMillis(),
                "the tick thread reads the published sample and its age, not a new pass");
            assertTrue(status.snapshot().pool(PoolId.REGION_EXECUTION).isPresent(),
                "the pool table is populated on a tick thread");
            assertTrue(status.unregisteredThreads() >= 0, "the unknown count is available from the cache");
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void aDisabledLayerNeverStartsTheRefresher() throws IOException {
        final ThreadsBootstrap bootstrap = new ThreadsBootstrap();
        bootstrap.startWith(new ThreadsConfig(this.write("""
            thread-management:
              enabled: false
            """)));
        try {
            assertNull(samplerThread(), "the kill switch must not create the refresher");
            assertNull(bootstrap.currentStatus().snapshot(), "and there is no sample to show");
        } finally {
            bootstrap.shutdown();
        }
    }

    private ThreadManagerRuntime runtime() {
        final ThreadManagerRuntime runtime = new DefaultThreadManagerFactory()
            .create(ThreadManagementSettings.defaults());
        runtime.start();
        return runtime;
    }

    private static void stop(final ThreadManagerRuntime runtime) {
        runtime.shutdown(Duration.ZERO);
        try {
            assertTrue(runtime.awaitTermination(Duration.ofSeconds(10L)));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private Path write(final String contents) throws IOException {
        final Path file = this.directory.resolve("tuff.yml");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        return file;
    }

    private static Thread samplerThread() {
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith("Tuff Threads Sampler")) {
                return thread;
            }
        }
        return null;
    }

    /**
     * Reports a fixed region summary, in the shape the Folia probe produces.
     */
    private static final class FixedRegionProbe implements PoolProbe {

        @Override
        public PoolId pool() {
            return PoolId.REGION_EXECUTION;
        }

        @Override
        public PoolObservation observe() {
            return new Reading(new RegionPoolMetrics(3, 4.2d, 9.5d, 4.4d, 12.0d, 19.5d, 1, 100, 2, 40));
        }
    }

    private record Reading(RegionPoolMetrics metrics) implements PoolObservation {

        @Override
        public int aliveThreads() {
            return 0;
        }

        @Override
        public int coreThreads() {
            return 0;
        }

        @Override
        public int activeThreads() {
            return 0;
        }

        @Override
        public int idleThreads() {
            return 0;
        }

        @Override
        public long queuedTasks() {
            return -1L;
        }

        @Override
        public long queueCapacity() {
            return -1L;
        }

        @Override
        public List<Thread> threads() {
            return List.of();
        }

        @Override
        public Optional<RegionPoolMetrics> regionMetrics() {
            return Optional.of(this.metrics);
        }
    }
}
