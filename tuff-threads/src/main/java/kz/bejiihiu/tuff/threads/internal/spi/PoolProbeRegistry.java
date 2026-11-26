package kz.bejiihiu.tuff.threads.internal.spi;

import kz.bejiihiu.tuff.threads.PoolId;

/**
 * Attachment point for pool probes.
 *
 * <p>The public registry stores specs; this internal seam stores the probes that turn a spec into live
 * numbers. They are separate on purpose: a plugin or a command may register a pool description, but only
 * the pool owner has the object that can observe it.
 *
 * <p>Attaching a probe for an unregistered pool is rejected. Attaching a second probe for the same pool
 * replaces the first, which makes reload idempotent. Detaching is idempotent and leaves the spec in
 * place.
 */
public interface PoolProbeRegistry {

    /**
     * Attaches a probe, replacing an existing one for the same pool.
     *
     * @param probe probe to attach
     * @throws NullPointerException  if {@code probe} is {@code null}
     * @throws IllegalStateException if the pool is not registered or the manager has stopped
     */
    void attach(PoolProbe probe);

    /**
     * Detaches the probe of a pool.
     *
     * <p>Idempotent. The pool stays registered and keeps its last known values until the metrics
     * retention expires; it just stops producing new ones.
     *
     * @param pool pool id
     * @return {@code true} if a probe was attached
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    boolean detach(PoolId pool);
}
