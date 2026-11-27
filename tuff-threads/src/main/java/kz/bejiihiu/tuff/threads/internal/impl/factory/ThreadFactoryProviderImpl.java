package kz.bejiihiu.tuff.threads.internal.impl.factory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadFactoryProvider;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;

/**
 * Default provider: builds platform threads from a spec, or borrows accounting for a thread somebody
 * else built.
 *
 * <p>The rules it enforces before doing anything: the pool must be registered, its coverage must be
 * {@link CoverageLevel#L2} or deeper, and the manager must still be running. A pool that is only
 * observed has no right to a provider created thread, and wrapping it is refused too, because the
 * registry entry would promise integration that does not exist.
 *
 * <p>Names are {@code <base>-<n>}, numbered per pool and base name, so two specs with the same base do
 * not collide. Everything else comes from the spec: daemon flag, priority, group, handler. The
 * provider never touches affinity, never picks a scheduling class and never starts a thread.
 *
 * <p>Virtual lanes are a kind, not a creation path. {@link #newThread(ThreadSpec, Runnable)} and
 * {@link #factory(ThreadSpec)} refuse {@link PoolKind#VIRTUAL}; a virtual thread is created by its
 * owner and only wrapped for accounting.
 *
 * <p>All state is a concurrent map of name counters, so the provider is safe under concurrent pool
 * bootstrap and does not allocate on any hot path of the server.
 */
public final class ThreadFactoryProviderImpl implements ThreadFactoryProvider {

    private final ExecutionPoolRegistry pools;
    private final ThreadRegistry registry;
    private final ConcurrentHashMap<String, AtomicInteger> sequences = new ConcurrentHashMap<>();
    private volatile boolean stopped;

    /**
     * Creates a provider.
     *
     * @param pools    pool registry used to check registration, coverage and kind
     * @param registry thread registry that receives every created or wrapped thread
     * @throws NullPointerException if a reference is {@code null}
     */
    public ThreadFactoryProviderImpl(final ExecutionPoolRegistry pools, final ThreadRegistry registry) {
        this.pools = Objects.requireNonNull(pools, "pools");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public Thread newThread(final ThreadSpec spec, final Runnable body) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(body, "body");
        this.requirePool(spec.pool(), true);

        final Runnable registered = () -> {
            try {
                body.run();
            } finally {
                // the thread is about to end, drop its reference so the registry keeps nothing dead
                this.registry.unregister(Thread.currentThread());
            }
        };
        final Thread thread = new Thread(spec.threadGroup(), registered, this.nextName(spec.pool(), spec.name()));
        thread.setDaemon(spec.daemon());
        thread.setPriority(spec.priority());
        thread.setUncaughtExceptionHandler(spec.uncaughtExceptionHandler());
        this.registry.register(thread, spec.pool());
        return thread;
    }

    @Override
    public ThreadFactory factory(final ThreadSpec spec) {
        Objects.requireNonNull(spec, "spec");
        this.requirePool(spec.pool(), true);

        return body -> this.newThread(spec, body);
    }

    @Override
    public Runnable wrap(final ThreadSpec spec, final Runnable body) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(body, "body");
        this.requirePool(spec.pool(), false);

        return () -> {
            final Thread current = Thread.currentThread();
            boolean registered;
            try {
                this.registry.register(current, spec.pool());
                registered = true;
            } catch (IllegalStateException ex) {
                // the wrapper outlived the layer: the registry stopped between building this runnable
                // and its first run. for a region body this is the tick path, so throwing here would
                // abort a tick over bookkeeping; run unaccounted instead. anything else is a real bug
                if (!this.registry.isStopped()) {
                    throw ex;
                }
                registered = false;
            }
            try {
                body.run();
            } finally {
                if (registered) {
                    this.registry.unregister(current);
                }
            }
        };
    }

    /**
     * Wraps an existing factory so every thread it builds is accounted while its body runs.
     *
     * <p>This is the adaptation path for pools whose threads cannot be built by the manager at all,
     * most notably Folia's region scheduler: the delegate keeps constructing its own subclass in its
     * own group with its own handler, and only the body it is given goes through
     * {@link #wrap(ThreadSpec, Runnable)}. Class, group, daemon flag and handler of the returned
     * thread are therefore exactly what the delegate produced, and registration happens when the body
     * starts rather than when the thread object is created.
     *
     * @param spec     registered pool description, only its pool is used
     * @param delegate factory that actually builds the threads
     * @return a factory that wraps the body it is handed before delegating
     * @throws NullPointerException  if a reference is {@code null}
     * @throws IllegalStateException if the pool is not registered or is not at
     *                               {@link CoverageLevel#L2} or deeper
     */
    public ThreadFactory wrapFactory(final ThreadSpec spec, final ThreadFactory delegate) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(delegate, "delegate");
        this.requirePool(spec.pool(), false);

        return body -> delegate.newThread(this.wrap(spec, body));
    }

    @Override
    public Optional<PoolId> poolOf(final Thread thread) {
        Objects.requireNonNull(thread, "thread");

        return this.registry.poolOf(thread);
    }

    /**
     * Refuses further creations and wrappers while keeping already created threads alive.
     *
     * <p>Called by the runtime when shutdown starts. Unregistering stays possible, otherwise bodies
     * finishing during shutdown would leak their entries.
     */
    public void markStopped() {
        this.stopped = true;
    }

    /**
     * Checks whether the provider refuses new threads.
     *
     * @return {@code true} after {@link #markStopped()}
     */
    public boolean isStopped() {
        return this.stopped;
    }

    private void requirePool(final PoolId pool, final boolean creating) {
        if (this.stopped) {
            throw new IllegalStateException("thread factory provider is stopped");
        }
        final PoolSpec spec = this.pools.spec(pool).orElseThrow(
            () -> new IllegalStateException("pool '" + pool + "' is not registered")
        );
        if (!spec.coverage().managesThreads()) {
            throw new IllegalStateException(
                "pool '" + pool + "' is at " + spec.coverage() + ", thread management needs L2 or deeper"
            );
        }
        if (creating && spec.kind() == PoolKind.VIRTUAL) {
            throw new IllegalStateException(
                "pool '" + pool + "' is a virtual lane, create virtual threads with Thread.ofVirtual and wrap them"
            );
        }
    }

    private String nextName(final PoolId pool, final String base) {
        final String stem = stem(base);
        final int sequence = this.sequences
            .computeIfAbsent(pool.value() + "/" + stem, ignored -> new AtomicInteger())
            .incrementAndGet();
        return stem + "-" + sequence;
    }

    private static String stem(final String base) {
        int end = base.length();
        while (end > 0) {
            final char last = base.charAt(end - 1);
            if (last != '-' && last != ' ' && last != '_') {
                break;
            }
            end--;
        }
        return end == 0 ? "thread" : base.substring(0, end);
    }
}
