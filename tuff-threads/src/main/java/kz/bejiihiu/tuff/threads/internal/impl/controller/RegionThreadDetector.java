package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.util.Objects;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;

/**
 * Tells the controller whether the calling thread is a region tick thread.
 *
 * <p>The controller needs this because a tick thread waiting for a lease is waiting for itself: permits
 * are per pool, ticks come from the region pool, and a blocked tick thread can never release the permit
 * it is parked on. A blocking request from such a thread is therefore an error, not a slow path.
 *
 * <p>The decision is a seam rather than a class check on purpose. This module cannot see Folia's thread
 * classes, so the primary answer comes from the thread registry: whatever is accounted to
 * {@code region-execution} is a tick thread, no matter what it is called. The name check exists only as
 * a fallback for threads nobody registered yet - the default integration level is observe-only, so Folia
 * threads are not in the ledger until the region pool is actually managed.
 */
@FunctionalInterface
public interface RegionThreadDetector {

    /**
     * Checks whether a thread runs region ticks.
     *
     * @param thread thread to check
     * @return {@code true} if the thread must never block on a lease
     */
    boolean isRegionThread(Thread thread);

    /**
     * Returns a detector that asks the registry first and the fallback second.
     *
     * <p>A registered thread is classified by its pool and nothing else: a plugin pool whose thread
     * happens to be called {@code Folia Region Scheduler ...} is not a tick thread, and a renamed tick
     * thread still is. The fallback only sees threads that no pool accounts for.
     *
     * @param threads  thread registry that knows which pool owns a thread
     * @param fallback detector used for unregistered threads
     * @return the detector
     * @throws NullPointerException if a reference is {@code null}
     */
    static RegionThreadDetector registered(final ThreadRegistry threads, final RegionThreadDetector fallback) {
        Objects.requireNonNull(threads, "threads");
        Objects.requireNonNull(fallback, "fallback");
        return thread -> {
            final Optional<PoolId> pool = threads.poolOf(thread);
            return pool.isPresent()
                ? pool.get().equals(PoolId.REGION_EXECUTION)
                : fallback.isRegionThread(thread);
        };
    }

    /**
     * Returns a detector that recognises Folia's region scheduler threads by name.
     *
     * <p>Folia names them {@code Folia Region Scheduler Thread #N}. Only the name is checked: thread
     * group membership is inherited from whoever created the thread, so matching the group flagged
     * ordinary pool threads that merely lived in Folia's group. This detector is a fallback, never the
     * primary answer.
     *
     * @return the detector
     */
    static RegionThreadDetector byThreadName() {
        return thread -> thread.getName().startsWith("Folia Region Scheduler Thread");
    }

    /**
     * Returns a detector that never recognises anything.
     *
     * <p>For tests and for platforms where region ticks are not threaded, like a single threaded
     * harness.
     *
     * @return the detector
     */
    static RegionThreadDetector none() {
        return thread -> false;
    }
}
