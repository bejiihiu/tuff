package kz.bejiihiu.tuff.threads.probe;

import ca.spottedleaf.moonrise.common.util.MoonriseCommon;
import java.util.Objects;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;

/**
 * Observe-only probe of the Moonrise chunk worker and chunk IO pools.
 *
 * <p>Both pools are already centralised: one {@code BalancedPrioritisedThreadPool} each, with a single
 * resize and a single halt. The probe reads their own thread arrays instead of scanning names, so the
 * server's chunk thread accounting stays the source of truth. Nothing is resized, stopped or
 * interrupted.
 *
 * <p>The pool has no queue depth or parallelism getter, so those values are {@code -1} ("unknown"),
 * never an invented zero. Core threads are the configured pool size and are the honest floor.
 */
public final class ChunkPoolProbe implements PoolProbe {

    private final PoolId pool;
    private final ChunkThreadSource source;

    /**
     * Creates the probe of the generation and maintenance workers.
     *
     * @return the chunk worker probe
     */
    public static ChunkPoolProbe worker() {
        return new ChunkPoolProbe(PoolId.CHUNK_WORKER, new ChunkThreadSource() {
            @Override
            public Thread[] aliveThreads() {
                return MoonriseCommon.WORKER_POOL.getAliveThreads();
            }

            @Override
            public int coreThreads() {
                return MoonriseCommon.WORKER_POOL.getCoreThreads().length;
            }
        });
    }

    /**
     * Creates the probe of the chunk file IO workers.
     *
     * @return the chunk IO probe
     */
    public static ChunkPoolProbe io() {
        return new ChunkPoolProbe(PoolId.CHUNK_IO, new ChunkThreadSource() {
            @Override
            public Thread[] aliveThreads() {
                return MoonriseCommon.IO_POOL.getAliveThreads();
            }

            @Override
            public int coreThreads() {
                return MoonriseCommon.IO_POOL.getCoreThreads().length;
            }
        });
    }

    /**
     * Creates a probe with an explicit source, for tests.
     *
     * @param pool   pool id, either chunk worker or chunk IO
     * @param source thread source
     * @throws NullPointerException if a reference is {@code null}
     */
    ChunkPoolProbe(final PoolId pool, final ChunkThreadSource source) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public PoolId pool() {
        return this.pool;
    }

    /**
     * Takes one reading of the pool.
     *
     * <p>Never throws: a pool that halted or a class that is gone reads as unavailable.
     *
     * @return the reading, never {@code null}
     */
    @Override
    public PoolObservation observe() {
        try {
            final Thread[] alive = this.source.aliveThreads();
            if (alive == null) {
                return ThreadPoolObservation.unavailable();
            }
            final int core;
            try {
                core = this.source.coreThreads();
            } catch (RuntimeException | LinkageError _) {
                // the thread list is still useful; only the floor is unknown
                return ThreadPoolObservation.fromThreads(
                    ThreadPoolObservation.nonNull(alive), 0, -1L, -1L
                );
            }
            return ThreadPoolObservation.fromThreads(
                ThreadPoolObservation.nonNull(alive), core, -1L, -1L
            );
        } catch (RuntimeException | LinkageError _) {
            return ThreadPoolObservation.unavailable();
        }
    }

    /**
     * Source of one Moonrise pool's threads.
     */
    interface ChunkThreadSource {

        /**
         * Returns the live threads of the pool.
         *
         * @return the threads, or {@code null} when the pool is gone
         */
        Thread[] aliveThreads();

        /**
         * Returns the configured number of threads.
         *
         * @return the core size, or {@code -1} when it cannot be read
         */
        default int coreThreads() {
            return -1;
        }
    }
}
