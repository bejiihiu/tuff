package kz.bejiihiu.tuff.threads.probe;

import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkPoolProbeTest {

    @Test
    void theFactoriesObserveTheCanonicalPools() {
        assertEquals(PoolId.CHUNK_WORKER, ChunkPoolProbe.worker().pool());
        assertEquals(PoolId.CHUNK_IO, ChunkPoolProbe.io().pool());
    }

    @Test
    void readsTheAliveAndCoreThreadCounts() {
        final ChunkPoolProbe probe = new ChunkPoolProbe(PoolId.CHUNK_WORKER, new ChunkPoolProbe.ChunkThreadSource() {
            @Override
            public Thread[] aliveThreads() {
                return threads(
                    thread("Tuff Common Worker #0", Thread.State.RUNNABLE),
                    thread("Tuff Common Worker #1", Thread.State.WAITING)
                );
            }

            @Override
            public int coreThreads() {
                return 4;
            }
        });

        final PoolObservation observation = probe.observe();

        assertEquals(2, observation.aliveThreads());
        assertEquals(4, observation.coreThreads(), "the configured pool size is the real floor");
        assertEquals(1, observation.activeThreads());
        assertEquals(1, observation.idleThreads());
        assertEquals(2, observation.threads().size());
        assertEquals(-1L, observation.queuedTasks(), "Moonrise does not expose queue depth");
        assertEquals(-1L, observation.queueCapacity());
    }

    @Test
    void aPoolThatIsGoneReportsUnknownActivity() {
        final ChunkPoolProbe probe = new ChunkPoolProbe(PoolId.CHUNK_IO, () -> {
            throw new IllegalStateException("the pool halted");
        });

        final PoolObservation observation = probe.observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(-1, observation.activeThreads(), "no source means unknown, not idle");
        assertEquals(-1, observation.idleThreads());
        assertEquals(-1L, observation.queuedTasks());
        assertTrue(observation.threads().isEmpty());
    }

    @Test
    void aNullThreadArrayIsUnavailable() {
        final PoolObservation observation = new ChunkPoolProbe(PoolId.CHUNK_WORKER, () -> null).observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(-1, observation.activeThreads());
    }

    @Test
    void aBrokenCoreReadStillKeepsTheThreadList() {
        final ChunkPoolProbe probe = new ChunkPoolProbe(PoolId.CHUNK_WORKER, new ChunkPoolProbe.ChunkThreadSource() {
            @Override
            public Thread[] aliveThreads() {
                return threads(thread("Tuff Common Worker #0", Thread.State.RUNNABLE));
            }

            @Override
            public int coreThreads() {
                throw new IllegalStateException("the configured size is gone");
            }
        });

        final PoolObservation observation = probe.observe();

        assertEquals(1, observation.aliveThreads());
        assertEquals(0, observation.coreThreads(), "the floor degrades to an honest zero");
        assertEquals(1, observation.threads().size());
    }

    private static Thread[] threads(final Thread... values) {
        return values;
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
