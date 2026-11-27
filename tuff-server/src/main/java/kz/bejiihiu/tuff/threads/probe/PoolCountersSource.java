package kz.bejiihiu.tuff.threads.probe;

import org.jspecify.annotations.Nullable;

/**
 * Source of a pool's own counters.
 *
 * <p>A {@code null} answer means the pool is absent or does not expose counters; the caller then falls
 * back to the name scan and reports {@code -1} for what it cannot measure. Tests inject fixed values.
 */
@FunctionalInterface
interface PoolCountersSource {

    /**
     * Returns the counters, or {@code null} when the pool cannot answer.
     *
     * @return the counters, or {@code null}
     */
    @Nullable PoolCounters counters();
}
