package kz.bejiihiu.tuff.threads.internal.spi;

import kz.bejiihiu.tuff.threads.PoolId;

/**
 * Adapter a pool owner implements so its pool can be observed.
 *
 * <p>A probe is attached to a registered pool by the owner during bootstrap and detached on shutdown.
 * It is the only integration a level {@code L1} pool needs: no thread factory, no budget, no lifecycle
 * handover, just counters and thread lists. Pools that fully delegate to the manager still usually
 * provide a probe, because the manager's own bookkeeping cannot see everything an owner knows (queue
 * depth, core size, in flight work).
 *
 * <p>The same probe instance may be called from the sampler thread while the pool shuts down. It must
 * return its last known values instead of throwing; a dead pool reports zeros, not an exception.
 */
public interface PoolProbe {

    /**
     * Returns the pool this probe observes.
     *
     * @return the pool id
     */
    PoolId pool();

    /**
     * Takes a reading of the pool.
     *
     * @return the reading, never {@code null}
     */
    PoolObservation observe();
}
