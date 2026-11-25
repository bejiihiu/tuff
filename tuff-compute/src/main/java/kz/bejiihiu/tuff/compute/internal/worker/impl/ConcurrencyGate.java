package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Limits how many workers may be inside {@code executeNext} at the same time.
 *
 * <p>The gate is the pool wide concurrency limit: a worker that cannot get a permit is idle and must
 * not touch the queue. Acquiring is a CAS on the fast path; only when the limit is reached does a
 * worker enter the monitor and wait, so the normal task path never takes a lock.
 *
 * <p>Lowering the limit does not revoke permits that were already handed out. Running tasks finish,
 * and new claims are blocked until the number of holders drops below the new limit.
 */
final class ConcurrencyGate {

    private final AtomicInteger limit;
    private final AtomicInteger inUse = new AtomicInteger();
    private final Object monitor = new Object();
    private volatile int waiters;
    private volatile boolean closed;

    /**
     * Creates a gate with the given initial limit.
     *
     * @param initialLimit initial number of permits, at least {@code 1}
     */
    ConcurrencyGate(final int initialLimit) {
        this.limit = new AtomicInteger(initialLimit);
    }

    /**
     * Returns the current limit.
     *
     * @return the number of permits
     */
    int limit() {
        return this.limit.get();
    }

    /**
     * Returns how many permits are currently held.
     *
     * @return the number of workers inside a task
     */
    int inUse() {
        return this.inUse.get();
    }

    /**
     * Changes the limit.
     *
     * <p>Raising it wakes every waiter so they can re-check the counter. Lowering it never touches a
     * permit that is already held.
     *
     * @param newLimit new number of permits, at least {@code 1}
     */
    void setLimit(final int newLimit) {
        this.limit.set(newLimit);
        this.signalAll();
    }

    /**
     * Tries to take a permit without waiting.
     *
     * @return {@code true} if a permit was taken
     */
    boolean tryAcquire() {
        while (true) {
            final int current = this.inUse.get();
            if (current >= this.limit.get()) {
                return false;
            }
            if (this.inUse.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Takes a permit, waiting up to the supplied time for one to become free.
     *
     * @param timeoutNanos maximum time to wait, zero or negative means "try once"
     * @return {@code true} if a permit was taken, {@code false} on timeout or when the gate is closed
     * @throws InterruptedException if the calling thread was interrupted while waiting
     */
    boolean acquire(final long timeoutNanos) throws InterruptedException {
        if (this.tryAcquire()) {
            return true;
        }
        final long deadline = System.nanoTime() + Math.max(0L, timeoutNanos);
        synchronized (this.monitor) {
            this.waiters++;
            try {
                while (true) {
                    if (this.closed) {
                        return false;
                    }
                    if (this.tryAcquire()) {
                        return true;
                    }
                    final long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        return false;
                    }
                    final long millis = remaining / 1_000_000L;
                    final int nanos = (int) (remaining % 1_000_000L);
                    this.monitor.wait(millis, nanos);
                }
            } finally {
                this.waiters--;
            }
        }
    }

    /**
     * Returns a permit.
     *
     * <p>A release that would leave permits unused wakes one waiter, so a saturated gate does not
     * have to wait for a timed poll after every task.
     */
    void release() {
        this.inUse.decrementAndGet();
        if (this.waiters > 0) {
            synchronized (this.monitor) {
                this.monitor.notify();
            }
        }
    }

    /**
     * Wakes every waiter so it can re-check the limit or notice the shutdown.
     */
    void signalAll() {
        if (this.waiters > 0) {
            synchronized (this.monitor) {
                this.monitor.notifyAll();
            }
        }
    }

    /**
     * Closes the gate to new permits.
     *
     * <p>Held permits stay valid; every waiter returns {@code false} and the closed state is
     * permanent.
     */
    void close() {
        this.closed = true;
        synchronized (this.monitor) {
            this.monitor.notifyAll();
        }
    }

    /**
     * Checks whether the gate was closed.
     *
     * @return {@code true} after {@link #close()}
     */
    boolean isClosed() {
        return this.closed;
    }
}
