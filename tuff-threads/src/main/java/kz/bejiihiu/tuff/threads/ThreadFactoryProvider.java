package kz.bejiihiu.tuff.threads;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;

/**
 * The only supported way to create a thread for a registered pool.
 *
 * <p>Three integration styles are covered, because existing pools differ in how much control they can
 * give up:
 * <ul>
 *     <li>{@link #newThread(ThreadSpec, Runnable)} and {@link #factory(ThreadSpec)} create ordinary
 *     {@link Thread} instances with a pool tag, a stable name, the requested daemon status, priority,
 *     group and handler, and register them for accounting. This is the normal path for new pools and
 *     for existing executors that accept a {@link ThreadFactory}.</li>
 *     <li>{@link #wrap(ThreadSpec, Runnable)} does not create anything. It wraps a body so that the
 *     thread which eventually runs it is registered under the pool for exactly the duration of the
 *     body. This is the path for threads whose concrete class must stay intact, most notably Folia's
 *     region tick threads: the owner keeps constructing its own subclass, its own thread group and its
 *     own uncaught handler, and only borrows the accounting.</li>
 * </ul>
 *
 * <p>The provider never changes a thread's class, never moves a running thread to another group, never
 * rewrites an existing uncaught exception handler and never sets affinity or a scheduling class. Names
 * are the only observable property it changes, and only on threads it created itself.
 *
 * <p>All methods are thread safe. They are meant for bootstrap and pool owners, not for per task code;
 * a factory call that has to allocate a thread is allowed to be slow but must not block on the pool it
 * serves.
 */
public interface ThreadFactoryProvider {

    /**
     * Creates a thread for a registered pool, without starting it.
     *
     * <p>The thread is registered immediately, so a snapshot taken between creation and
     * {@link Thread#start()} already counts it. Its name is the base name from the spec plus a unique
     * suffix; daemon status, priority, group and handler are copied from the spec. The body runs exactly
     * once.
     *
     * <p>The caller owns the returned thread and must start it, or let it be garbage collected. The
     * manager never starts a thread on its own except inside its own managed lanes.
     *
     * @param spec registered pool description describing the thread
     * @param body work the thread runs
     * @return the created, not yet started thread
     * @throws NullPointerException  if a reference is {@code null}
     * @throws IllegalStateException if the manager is not running, the pool is not registered or the
     *                               pool is not at {@link CoverageLevel#L2} or deeper
     */
    Thread newThread(ThreadSpec spec, Runnable body);

    /**
     * Returns a {@link ThreadFactory} that applies a spec to every thread it creates.
     *
     * <p>The returned factory behaves like {@link #newThread(ThreadSpec, Runnable)}: each call creates
     * and registers one thread and appends a unique suffix to the base name. It is the adapter for
     * existing pools that already accept a {@code ThreadFactory} and keep their own executor.
     *
     * @param spec registered pool description describing the threads
     * @return a factory bound to the spec
     * @throws NullPointerException  if {@code spec} is {@code null}
     * @throws IllegalStateException if the pool is not registered or is not at
     *                               {@link CoverageLevel#L2} or deeper
     */
    ThreadFactory factory(ThreadSpec spec);

    /**
     * Wraps a body so the thread running it is accounted under the pool.
     *
     * <p>On entry the wrapper registers {@link Thread#currentThread()} under {@link ThreadSpec#pool()}
     * and on exit, including exceptional exit, it unregisters it. Nothing else is touched: the name,
     * daemon status, priority, group, handler and concrete class of the thread stay exactly as the
     * caller made them. Only {@link ThreadSpec#pool()} is read from the spec; the other properties are
     * documentation of what the caller already applied.
     *
     * <p>If the same thread runs several wrapped bodies, registration is reference counted per thread
     * and the thread stays registered until the outermost body returns. Wrapping a body twice is
     * therefore safe, and a pool that reuses threads does not lose accounting between tasks.
     *
     * @param spec registered pool description, only its pool is used
     * @param body work to run
     * @return a wrapper that registers, runs and unregisters
     * @throws NullPointerException  if a reference is {@code null}
     * @throws IllegalStateException if the pool is not registered or is not at
     *                               {@link CoverageLevel#L2} or deeper
     */
    Runnable wrap(ThreadSpec spec, Runnable body);

    /**
     * Looks up the pool a live thread is accounted to.
     *
     * <p>This is a read only convenience for diagnostics and ownership checks. It returns empty for
     * threads the provider did not create or wrap, including JDK, logger and third party threads.
     *
     * @param thread thread to look up
     * @return the pool id, or an empty optional if the thread is not accounted
     * @throws NullPointerException if {@code thread} is {@code null}
     */
    Optional<PoolId> poolOf(Thread thread);
}
