package kz.bejiihiu.tuff.threads.internal.impl.registry;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;

/**
 * Default thread registry: a concurrent map from thread to owning pool plus per pool counters.
 *
 * <p>Registration is a single {@code putIfAbsent}, unregistration is a single reference count decrement
 * and removal, and pool lookups are one map read. Nothing here scans the pool or the JVM, so a pool can
 * create threads at its own pace without turning creation into an O(n) operation.
 *
 * <p>Counters are additive per pool and are never reset, not even by a pool leaving the registry: the
 * metrics layer needs the history for rates. A thread counts as created the first time it is registered
 * and as terminated when its last reference is released.
 *
 * <p>The event sink is called from the registering and unregistering thread. A sink throwing is not
 * allowed to break accounting, so exceptions from it are swallowed; the sink contract already says it
 * must not throw.
 */
public final class ThreadRegistryImpl implements ThreadRegistry {

    private final ConcurrentHashMap<Thread, Entry> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PoolId, Set<Thread>> byPool = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PoolId, LongAdder> created = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PoolId, LongAdder> terminated = new ConcurrentHashMap<>();
    private final ThreadMXBean threadBean;
    private final ThreadEventSink sink;
    private volatile boolean stopped;

    /**
     * Creates a registry.
     *
     * @param sink event sink called on creation and termination, must not throw
     * @throws NullPointerException if {@code sink} is {@code null}
     */
    public ThreadRegistryImpl(final ThreadEventSink sink) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.threadBean = ManagementFactory.getThreadMXBean();
    }

    @Override
    public void register(final Thread thread, final PoolId pool) {
        Objects.requireNonNull(thread, "thread");
        Objects.requireNonNull(pool, "pool");
        if (this.stopped) {
            throw new IllegalStateException("thread registry is stopped");
        }

        final Entry existing = this.entries.putIfAbsent(thread, new Entry(pool));
        if (existing == null) {
            // a set per pool keeps threads(pool) proportional to the pool, not to the whole registry
            this.byPool.computeIfAbsent(pool, ignored -> ConcurrentHashMap.newKeySet()).add(thread);
            this.counter(this.created, pool).increment();
            this.emitCreated(thread, pool);
            return;
        }
        if (!existing.pool.equals(pool)) {
            throw new IllegalStateException(
                "thread '" + thread.getName() + "' belongs to pool '" + existing.pool
                    + "' and cannot be registered under '" + pool + "'"
            );
        }
        existing.retain();
    }

    @Override
    public void unregister(final Thread thread) {
        Objects.requireNonNull(thread, "thread");

        final Entry entry = this.entries.get(thread);
        if (entry == null) {
            // deliberately idempotent: a body may run cleanup without having registered first
            return;
        }
        if (entry.release() > 0) {
            return;
        }
        if (!this.entries.remove(thread, entry)) {
            return;
        }
        final Set<Thread> poolThreads = this.byPool.get(entry.pool);
        if (poolThreads != null) {
            poolThreads.remove(thread);
        }
        this.counter(this.terminated, entry.pool).increment();
        this.emitTerminated(thread, entry.pool);
    }

    @Override
    public Optional<PoolId> poolOf(final Thread thread) {
        Objects.requireNonNull(thread, "thread");

        final Entry entry = this.entries.get(thread);
        return entry == null ? Optional.empty() : Optional.of(entry.pool);
    }

    @Override
    public List<Thread> threads(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");

        final Set<Thread> poolThreads = this.byPool.get(pool);
        if (poolThreads == null || poolThreads.isEmpty()) {
            return List.of();
        }
        final List<Thread> live = new ArrayList<>(poolThreads.size());
        for (final Thread thread : poolThreads) {
            if (!this.reapTerminated(pool, thread)) {
                live.add(thread);
            }
        }
        live.sort(Comparator.comparingLong(Thread::threadId));
        return List.copyOf(live);
    }

    @Override
    public int aliveThreads(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");

        final Set<Thread> poolThreads = this.byPool.get(pool);
        if (poolThreads == null) {
            return 0;
        }
        int alive = 0;
        for (final Thread thread : poolThreads) {
            if (!this.reapTerminated(pool, thread)) {
                alive++;
            }
        }
        return alive;
    }

    @Override
    public long createdTotal(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");

        final LongAdder counter = this.created.get(pool);
        return counter == null ? 0L : counter.sum();
    }

    @Override
    public long terminatedTotal(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");

        final LongAdder counter = this.terminated.get(pool);
        return counter == null ? 0L : counter.sum();
    }

    @Override
    public List<ThreadSnapshot> snapshot() {
        final List<ThreadSnapshot> snapshots = new ArrayList<>(this.entries.size());
        for (final Map.Entry<Thread, Entry> entry : this.entries.entrySet()) {
            final Thread thread = entry.getKey();
            final Entry value = entry.getValue();
            // defensive reap: threads registered directly, without the factory wrapping the body,
            // would otherwise sit here forever. a dead thread is never coming back.
            if (this.reapDead(thread, value)) {
                continue;
            }
            snapshots.add(this.snapshotOf(thread, value.pool));
        }
        snapshots.sort(Comparator.comparingLong(ThreadSnapshot::threadId));
        return List.copyOf(snapshots);
    }

    /**
     * Refuses further registrations while still allowing unregistration.
     *
     * <p>Called by the runtime during shutdown. Threads that exist already keep running, their bodies
     * still release their references, and snapshots keep working until the last one is gone.
     */
    public void markStopped() {
        this.stopped = true;
    }

    /**
     * Checks whether registrations are refused.
     *
     * @return {@code true} after {@link #markStopped()}
     */
    public boolean isStopped() {
        return this.stopped;
    }

    private LongAdder counter(final ConcurrentHashMap<PoolId, LongAdder> counters, final PoolId pool) {
        return counters.computeIfAbsent(pool, ignored -> new LongAdder());
    }

    private ThreadSnapshot snapshotOf(final Thread thread, final PoolId pool) {
        return new ThreadSnapshot(
            thread.threadId(),
            thread.getName(),
            pool,
            thread.getState(),
            thread.isDaemon(),
            thread.getPriority(),
            this.cpuTimeNanos(thread)
        );
    }

    private long cpuTimeNanos(final Thread thread) {
        if (!this.threadBean.isThreadCpuTimeSupported() || !this.threadBean.isThreadCpuTimeEnabled()) {
            return -1L;
        }
        final long nanos = this.threadBean.getThreadCpuTime(thread.threadId());
        return nanos < 0L ? -1L : nanos;
    }

    private boolean reapDead(final Thread thread, final Entry entry) {
        if (thread.getState() != Thread.State.TERMINATED || !this.entries.remove(thread, entry)) {
            return false;
        }
        final Set<Thread> poolThreads = this.byPool.get(entry.pool);
        if (poolThreads != null) {
            poolThreads.remove(thread);
        }
        this.counter(this.terminated, entry.pool).increment();
        this.emitTerminated(thread, entry.pool);
        return true;
    }

    private boolean reapTerminated(final PoolId pool, final Thread thread) {
        if (thread.getState() != Thread.State.TERMINATED) {
            return false;
        }
        final Entry entry = this.entries.get(thread);
        if (entry == null) {
            // stale set membership left by a racing unregister; the counter already moved there
            final Set<Thread> poolThreads = this.byPool.get(pool);
            if (poolThreads != null) {
                poolThreads.remove(thread);
            }
            return true;
        }
        return this.reapDead(thread, entry);
    }

    private void emitCreated(final Thread thread, final PoolId pool) {
        try {
            this.sink.threadCreated(this.snapshotOf(thread, pool));
        } catch (RuntimeException _) {
            // a broken observer must not break thread creation
        }
    }

    private void emitTerminated(final Thread thread, final PoolId pool) {
        try {
            this.sink.threadTerminated(this.snapshotOf(thread, pool));
        } catch (RuntimeException _) {
            // same deal, accounting already happened
        }
    }

    /**
     * Per thread bookkeeping: owning pool and a reference count.
     *
     * <p>All mutations happen inside map operations, which are atomic per key, so the counter is a
     * plain atomic rather than a lock. {@link #retain()} matches {@link #release()}.
     */
    private static final class Entry {

        private final PoolId pool;
        private final AtomicInteger references = new AtomicInteger(1);

        private Entry(final PoolId pool) {
            this.pool = pool;
        }

        private void retain() {
            this.references.incrementAndGet();
        }

        private int release() {
            return this.references.decrementAndGet();
        }
    }
}
