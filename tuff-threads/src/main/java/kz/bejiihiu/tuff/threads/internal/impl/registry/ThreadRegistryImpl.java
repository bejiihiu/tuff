package kz.bejiihiu.tuff.threads.internal.impl.registry;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;

/**
 * Default thread registry: a concurrent map from thread to owning pool plus per pool counters.
 *
 * <p>Registration is a single {@code putIfAbsent} under a small lifecycle monitor, unregistration is a
 * single reference count decrement and removal, and pool lookups are one map read. Nothing here scans
 * the pool or the JVM on the steady state path, so a pool can create threads at its own pace without
 * turning creation into an O(n) operation.
 *
 * <p>The monitor exists for one reason: the stop flag and the insertion must be decided together. A
 * register that passes the flag check and then lands after {@link #markStopped()} would resurrect
 * accounting in a manager that is already shutting down, and that thread would be in nobody's join.
 *
 * <p>Counters are additive per pool and are never reset, not even by a pool leaving the registry: the
 * metrics layer needs the history for rates. A thread counts as created the first time it is registered
 * and as terminated when its last reference is released.
 *
 * <p>Reaping: a terminated thread is dropped on the next read, and a thread that was registered but
 * never started is dropped once {@code newThreadGrace} has passed - the factory registers at creation,
 * and "created and never started" must not become a permanent strong reference. Reaping also runs from
 * {@link #register}, {@link #unregister} and {@link #markStopped}, throttled to {@code reapInterval}, so
 * it still happens when nobody reads snapshots ({@code metrics.enabled: false}) without turning thread
 * creation into a scan of the whole registry.
 *
 * <p>The event sink is called from the registering and unregistering thread. A sink throwing is not
 * allowed to break accounting, so exceptions from it are swallowed; the sink contract already says it
 * must not throw.
 */
public final class ThreadRegistryImpl implements ThreadRegistry {

    /**
     * How long a registered but never started thread is kept before it is treated as abandoned.
     */
    static final Duration DEFAULT_NEW_THREAD_GRACE = Duration.ofMinutes(5);

    /**
     * Minimum time between two automatic reap passes driven by register and unregister.
     */
    static final Duration DEFAULT_REAP_INTERVAL = Duration.ofSeconds(1);

    private final ConcurrentHashMap<Thread, Entry> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PoolId, Set<Thread>> byPool = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PoolId, LongAdder> created = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PoolId, LongAdder> terminated = new ConcurrentHashMap<>();
    private final ThreadMXBean threadBean;
    private final ThreadEventSink sink;
    private final Object lifecycle = new Object();
    private final long newThreadGraceNanos;
    private final long reapIntervalNanos;
    private final AtomicLong lastReapNanos = new AtomicLong(Long.MIN_VALUE);
    private volatile boolean stopped;

    /**
     * Creates a registry with the production grace and reap interval.
     *
     * @param sink event sink called on creation and termination, must not throw
     * @throws NullPointerException if {@code sink} is {@code null}
     */
    public ThreadRegistryImpl(final ThreadEventSink sink) {
        this(sink, DEFAULT_NEW_THREAD_GRACE, DEFAULT_REAP_INTERVAL);
    }

    /**
     * Creates a registry with an explicit grace and reap interval.
     *
     * <p>Package private because only tests need to shorten the grace; a server should keep the
     * production numbers.
     *
     * @param sink           event sink called on creation and termination, must not throw
     * @param newThreadGrace how long an unstarted thread may sit in the registry
     * @param reapInterval   minimum time between automatic reap passes
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a duration is negative
     */
    ThreadRegistryImpl(
        final ThreadEventSink sink,
        final Duration newThreadGrace,
        final Duration reapInterval
    ) {
        this.sink = Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(newThreadGrace, "newThreadGrace");
        Objects.requireNonNull(reapInterval, "reapInterval");
        if (newThreadGrace.isNegative()) {
            throw new IllegalArgumentException("newThreadGrace must not be negative, got " + newThreadGrace);
        }
        if (reapInterval.isNegative()) {
            throw new IllegalArgumentException("reapInterval must not be negative, got " + reapInterval);
        }
        this.newThreadGraceNanos = saturatingNanos(newThreadGrace);
        this.reapIntervalNanos = saturatingNanos(reapInterval);
        this.threadBean = ManagementFactory.getThreadMXBean();
    }

    @Override
    public void register(final Thread thread, final PoolId pool) {
        Objects.requireNonNull(thread, "thread");
        Objects.requireNonNull(pool, "pool");
        final boolean createdNow;
        synchronized (this.lifecycle) {
            if (this.stopped) {
                throw new IllegalStateException("thread registry is stopped");
            }
            final Entry existing = this.entries.putIfAbsent(thread, new Entry(pool, System.nanoTime()));
            if (existing == null) {
                // a set per pool keeps threads(pool) proportional to the pool, not to the whole registry
                this.byPool.computeIfAbsent(pool, ignored -> ConcurrentHashMap.newKeySet()).add(thread);
                this.counter(this.created, pool).increment();
                createdNow = true;
            } else if (!existing.pool.equals(pool)) {
                throw new IllegalStateException(
                    "thread '" + thread.getName() + "' belongs to pool '" + existing.pool
                        + "' and cannot be registered under '" + pool + "'"
                );
            } else {
                existing.retain();
                createdNow = false;
            }
        }
        if (createdNow) {
            this.emitCreated(thread, pool);
        }
        this.maybeReap();
    }

    @Override
    public void unregister(final Thread thread) {
        Objects.requireNonNull(thread, "thread");

        final Entry entry = this.entries.get(thread);
        if (entry == null) {
            // deliberately idempotent: a body may run cleanup without having registered first
            this.maybeReap();
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
        this.maybeReap();
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
        final long now = System.nanoTime();
        final List<Thread> live = new ArrayList<>(poolThreads.size());
        for (final Thread thread : poolThreads) {
            if (!this.reapStale(pool, thread, now)) {
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
        final long now = System.nanoTime();
        int alive = 0;
        for (final Thread thread : poolThreads) {
            if (!this.reapStale(pool, thread, now)) {
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
        final long now = System.nanoTime();
        final List<ThreadSnapshot> snapshots = new ArrayList<>(this.entries.size());
        for (final Map.Entry<Thread, Entry> entry : this.entries.entrySet()) {
            final Thread thread = entry.getKey();
            final Entry value = entry.getValue();
            // defensive reap: threads registered directly, without the factory wrapping the body,
            // would otherwise sit here forever. a dead thread is never coming back, and a thread that
            // was never started is not coming back either once the grace ran out.
            if (this.reapStale(thread, value, now)) {
                continue;
            }
            snapshots.add(this.snapshotOf(thread, value.pool));
        }
        snapshots.sort(Comparator.comparingLong(ThreadSnapshot::threadId));
        return List.copyOf(snapshots);
    }

    /**
     * Runs one reap pass, ignoring the automatic interval.
     *
     * <p>Drops terminated threads and threads that were registered but never started before the grace
     * period. Safe to call from any thread; meant for shutdown and for a periodic audit, not for a tick.
     *
     * @return how many entries were dropped
     */
    public int reap() {
        final long now = System.nanoTime();
        int removed = 0;
        for (final Map.Entry<Thread, Entry> entry : this.entries.entrySet()) {
            if (this.reapStale(entry.getKey(), entry.getValue(), now)) {
                removed++;
            }
        }
        return removed;
    }

    /**
     * Refuses further registrations while still allowing unregistration.
     *
     * <p>Called by the runtime during shutdown. Threads that exist already keep running, their bodies
     * still release their references, and snapshots keep working until the last one is gone.
     */
    public void markStopped() {
        synchronized (this.lifecycle) {
            this.stopped = true;
        }
        this.reap();
    }

    /**
     * Checks whether registrations are refused.
     *
     * @return {@code true} after {@link #markStopped()}
     */
    public boolean isStopped() {
        return this.stopped;
    }

    /**
     * Returns how many entries the registry currently holds, without reaping.
     *
     * <p>Package private on purpose: a test needs to see that the hook-based reap works when no read
     * ever happens, and a public accessor for the raw map size would invite treating it as a metric.
     *
     * @return entry count including entries a read would drop
     */
    int trackedThreads() {
        return this.entries.size();
    }

    private void maybeReap() {
        final long now = System.nanoTime();
        final long last = this.lastReapNanos.get();
        if (last != Long.MIN_VALUE && now - last < this.reapIntervalNanos) {
            return;
        }
        if (this.lastReapNanos.compareAndSet(last, now)) {
            this.reap();
        }
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

    /**
     * Drops an entry whose thread is gone or was never started in time.
     *
     * @param pool   pool the thread was registered under
     * @param thread thread to check
     * @param now    current monotonic time
     * @return {@code true} when the entry was dropped
     */
    private boolean reapStale(final PoolId pool, final Thread thread, final long now) {
        final Entry entry = this.entries.get(thread);
        if (entry == null) {
            // stale set membership left by a racing unregister; the counter already moved there
            final Set<Thread> poolThreads = this.byPool.get(pool);
            if (poolThreads != null) {
                poolThreads.remove(thread);
            }
            return true;
        }
        return this.reapStale(thread, entry, now);
    }

    private boolean reapStale(final Thread thread, final Entry entry, final long now) {
        final Thread.State state = thread.getState();
        final boolean dead = state == Thread.State.TERMINATED;
        final boolean abandoned = state == Thread.State.NEW && now - entry.createdNanos > this.newThreadGraceNanos;
        if (!dead && !abandoned) {
            return false;
        }
        return this.reapNow(thread, entry);
    }

    /**
     * Removes one entry and moves the counters.
     *
     * @param thread thread to remove
     * @param entry  entry that was read from the map
     * @return {@code true} when this call removed the entry
     */
    private boolean reapNow(final Thread thread, final Entry entry) {
        if (!this.entries.remove(thread, entry)) {
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

    private static long saturatingNanos(final Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException _) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Per thread bookkeeping: owning pool, registration moment and a reference count.
     *
     * <p>All mutations happen inside map operations, which are atomic per key, so the counter is a
     * plain atomic rather than a lock. {@link #retain()} matches {@link #release()}.
     */
    private static final class Entry {

        private final PoolId pool;
        private final long createdNanos;
        private final AtomicInteger references = new AtomicInteger(1);

        private Entry(final PoolId pool, final long createdNanos) {
            this.pool = pool;
            this.createdNanos = createdNanos;
        }

        private void retain() {
            this.references.incrementAndGet();
        }

        private int release() {
            return this.references.decrementAndGet();
        }
    }
}
