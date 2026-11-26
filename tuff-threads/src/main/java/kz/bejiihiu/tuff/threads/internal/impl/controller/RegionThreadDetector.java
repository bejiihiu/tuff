package kz.bejiihiu.tuff.threads.internal.impl.controller;

/**
 * Tells the controller whether the calling thread is a region tick thread.
 *
 * <p>The controller needs this because a tick thread waiting for a lease is waiting for itself: permits
 * are per pool, ticks come from the region pool, and a blocked tick thread can never release the permit
 * it is parked on. A blocking request from such a thread is therefore an error, not a slow path.
 *
 * <p>The decision is a seam rather than a class check on purpose. This module cannot see Folia's thread
 * classes, and hard coding a name would break the moment the scheduler is replaced. The default
 * implementation matches the names Folia gives its runner threads, which is good enough until the
 * thread registry can answer the same question from its ledger.
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
     * Returns a detector that recognises Folia's region scheduler threads by name.
     *
     * <p>Folia names them {@code Folia Region Scheduler Thread #N} inside a
     * {@code Folia Region Scheduler ThreadGroup}, so checking the prefix covers both the thread and any
     * future subclass that keeps the naming.
     *
     * @return the detector
     */
    static RegionThreadDetector byThreadName() {
        return thread -> {
            final ThreadGroup group = thread.getThreadGroup();
            return thread.getName().startsWith("Folia Region Scheduler Thread")
                || (group != null && group.getName().startsWith("Folia Region Scheduler"));
        };
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
