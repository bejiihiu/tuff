package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The name scan that turns live threads into the compute pool's thread list.
 *
 * <p>The engine names its threads in three families: platform workers, lane threads and the metrics
 * sampler. The workers and the sampler belong to the platform pool; the virtual lane's pump and the
 * completion threads belong to the virtual pool and must not be counted twice. No engine is started:
 * the test owns the threads and the scan is a pure name filter.
 */
@Timeout(60)
class ComputePoolProbeTest {

    private static final String WORKER_PREFIX = "Tuff Compute Worker";

    @Test
    void theMetricsSamplerBelongsToThePlatformPool() throws InterruptedException {
        final List<Thread> scanned = scanWith("Tuff Compute Worker-91", "Tuff Compute Metrics-91");

        assertTrue(byName(scanned, "Tuff Compute Worker-91"), "the workers are the pool");
        assertTrue(byName(scanned, "Tuff Compute Metrics-91"),
            "the engine's sampler is a compute thread and must not be left unknown");
    }

    @Test
    void theVirtualLaneThreadsStayOutOfThePlatformPool() throws InterruptedException {
        final List<Thread> scanned = scanWith(
            "Tuff Compute Worker-virtual-pump-91",
            "Tuff Compute Worker-completion-pool-91"
        );

        assertFalse(byName(scanned, "Tuff Compute Worker-virtual-pump-91"));
        assertFalse(byName(scanned, "Tuff Compute Worker-completion-pool-91"));
    }

    private static List<Thread> scanWith(final String... names) throws InterruptedException {
        final CountDownLatch started = new CountDownLatch(names.length);
        final CountDownLatch release = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>(names.length);
        for (final String name : names) {
            final Thread thread = new Thread(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }, name);
            thread.setDaemon(true);
            thread.start();
            threads.add(thread);
        }
        assertTrue(started.await(5L, TimeUnit.SECONDS), "the fake compute threads must reach their bodies");
        try {
            return ComputePoolProbe.scan(WORKER_PREFIX);
        } finally {
            release.countDown();
            for (final Thread thread : threads) {
                thread.join(5_000L);
            }
        }
    }

    private static boolean byName(final List<Thread> threads, final String name) {
        for (final Thread thread : threads) {
            if (thread.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
