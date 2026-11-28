package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class BackgroundPoolProbeTest {

    @Test
    void observesTheCanonicalBackgroundPool() {
        assertEquals(PoolId.BACKGROUND, new BackgroundPoolProbe().pool());
    }

    @Test
    void readsTheForkJoinCountersAndTheNamedWorkers() {
        final BackgroundPoolProbe probe = new BackgroundPoolProbe(
            () -> new PoolCounters(6, 4, 3, 2L),
            () -> List.of(
                thread("Worker-Main-1", Thread.State.RUNNABLE),
                thread("Worker-Main-2", Thread.State.RUNNABLE),
                thread("Worker-Main-3", Thread.State.WAITING),
                thread("Worker-Main-4", Thread.State.WAITING),
                thread("Worker-Main-5", Thread.State.TIMED_WAITING),
                thread("Worker-Main-6", Thread.State.TIMED_WAITING),
                thread("Netty NIO IO #0", Thread.State.RUNNABLE)
            )
        );

        final PoolObservation observation = probe.observe();

        assertEquals(6, observation.aliveThreads(), "a fork-join pool may exceed its parallelism when blocked");
        assertEquals(4, observation.coreThreads(), "parallelism is the real floor");
        assertEquals(3, observation.activeThreads());
        assertEquals(3, observation.idleThreads());
        assertEquals(2L, observation.queuedTasks());
        assertEquals(-1L, observation.queueCapacity());
        assertEquals(6, observation.threads().size());
    }

    @Test
    void fallsBackToNamesWhenTheExecutorIsSynchronous() {
        final BackgroundPoolProbe probe = new BackgroundPoolProbe(
            () -> null,
            () -> List.of(
                thread("Worker-Main-1", Thread.State.RUNNABLE),
                thread("Worker-Main-2", Thread.State.WAITING),
                thread("Other Worker", Thread.State.RUNNABLE)
            )
        );

        final PoolObservation observation = probe.observe();

        assertEquals(2, observation.aliveThreads());
        assertEquals(1, observation.activeThreads());
        assertEquals(1, observation.idleThreads());
        assertEquals(-1L, observation.queuedTasks());
    }

    @Test
    void nothingReadableReportsUnknownActivity() {
        final BackgroundPoolProbe probe = new BackgroundPoolProbe(
            () -> {
                throw new IllegalStateException("the executor went away");
            },
            () -> {
                throw new IllegalStateException("the thread table went away");
            }
        );

        final PoolObservation observation = probe.observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(-1, observation.activeThreads());
        assertEquals(-1, observation.idleThreads());
        assertTrue(observation.threads().isEmpty());
    }

    private static Thread thread(final String name, final Thread.State state) {
        return new Thread(name) {
            @Override
            public Thread.State getState() {
                return state;
            }
        };
    }
}
