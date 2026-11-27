package kz.bejiihiu.tuff.threads.internal.spi;

import java.util.List;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;

/**
 * Live bookkeeping of the threads the manager knows about.
 *
 * <p>The registry is the source of truth for "which pool owns this thread" and for the counters behind
 * creation and termination rates. It is fed by the factory, by wrapped bodies and by probes; nothing
 * else registers threads. A thread is registered at creation (or at the first moment a wrapped body
 * runs) and unregistered when it exits.
 *
 * <p>Registration is reference counted per thread so that a thread reused by a pool, or a body wrapped
 * twice, stays registered until the outermost unit of work ends. Registering a thread under a second
 * pool is a programming error and is rejected: a thread belongs to exactly one pool for its whole life.
 *
 * <p>All methods are thread safe and cheap. Snapshot methods read thread state and CPU time from the
 * JVM and allocate one record per thread; they are meant for the sampler and for commands, not for a
 * tick loop.
 */
public interface ThreadRegistry {

    /**
     * Registers a thread under a pool or increments its reference count.
     *
     * @param thread thread to register
     * @param pool   pool that owns it
     * @throws NullPointerException  if a reference is {@code null}
     * @throws IllegalStateException if the thread is already registered under a different pool or the
     *                               manager has stopped
     */
    void register(Thread thread, PoolId pool);

    /**
     * Decrements a thread's reference count and removes it when it reaches zero.
     *
     * <p>Idempotent for unknown threads: a thread that was never registered, or already left, is
     * ignored. This matters because a crashed body may skip registration but still run the cleanup.
     *
     * @param thread thread to unregister
     * @throws NullPointerException if {@code thread} is {@code null}
     */
    void unregister(Thread thread);

    /**
     * Looks up the pool of a thread.
     *
     * @param thread thread to look up
     * @return the pool id, or an empty optional if the thread is not registered
     * @throws NullPointerException if {@code thread} is {@code null}
     */
    Optional<PoolId> poolOf(Thread thread);

    /**
     * Returns the live threads of one pool.
     *
     * @param pool pool id
     * @return an immutable list of live threads, possibly empty
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    List<Thread> threads(PoolId pool);

    /**
     * Returns the number of live threads of one pool.
     *
     * @param pool pool id
     * @return the count, zero for an unknown or empty pool
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    int aliveThreads(PoolId pool);

    /**
     * Returns how many threads of a pool were created since the manager started.
     *
     * <p>Monotonic, never decremented, and kept for pools that were unregistered and registered again.
     *
     * @param pool pool id
     * @return the created count
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    long createdTotal(PoolId pool);

    /**
     * Returns how many threads of a pool terminated since the manager started.
     *
     * @param pool pool id
     * @return the terminated count
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    long terminatedTotal(PoolId pool);

    /**
     * Takes a snapshot of every registered thread.
     *
     * @return an immutable list in thread id order, possibly empty
     */
    List<ThreadSnapshot> snapshot();

    /**
     * Checks whether the registry has stopped accepting registrations.
     *
     * <p>A wrapper built before shutdown can run after it; it asks here to tell "the manager is
     * stopping" apart from a programming error, and runs its body unaccounted in the first case. The
     * default answers {@code false}, for registries that have no lifecycle.
     *
     * @return {@code true} after the owner stopped the registry
     */
    default boolean isStopped() {
        return false;
    }
}
