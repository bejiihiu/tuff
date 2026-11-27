package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.common.util.TimeUtil;
import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import ca.spottedleaf.concurrentutil.scheduler.Scheduler;
import ca.spottedleaf.concurrentutil.set.LinkedSortedSet;
import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.LazyRunnable;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Deadline-ordered tick scheduler with an elastic fund of threads.
 *
 * <p>The queue, the comparator and the runner state machine are ported one to one from
 * {@code ca.spottedleaf.concurrentutil.scheduler.EDFSchedulerThreadPool} (leafpile 1.3.3), so tick
 * order, tie breaks and the "a late region does not starve the rest" property are exactly Folia's.
 *
 * <p>The fund is the part that differs. {@code min} runners exist at all times; the supervisor, a
 * single daemon thread, may add runners up to {@code max} while ticks are waiting and retire idle
 * ones back down to {@code min}. A live runner is never halted, restarted or migrated: growth appends
 * a slot, shrink only retires a runner that is parked with no permit. Retired threads stay in the
 * join list, so shutdown still waits for every thread that ever existed (R13 of the rollout plan).
 *
 * <p>Elasticity is off by default ({@link KairosTuning#fixed()}), in which case there is no
 * supervisor and the fund behaves exactly like the first prototype: {@code min == max}.
 *
 * <p>Two deliberate deviations from the port, both forced by the package boundary:
 * <ul>
 *     <li>{@link SchedulableTick#getScheduledStart()} and {@code setState} are package private in
 *     leafpile, so the deadline arrives through a {@link ToLongFunction} supplied by the facade and
 *     per-tick ownership is tracked in an {@link IdentityHashMap} instead of the tick's own state
 *     field. Reads still happen on every comparison, so a tick that moves its own deadline is seen
 *     immediately, exactly like the original.</li>
 *     <li>{@code notifyTasks} stays a no op, which is the EDF contract: region tasks are drained at
 *     the start of the tick, never between ticks.</li>
 * </ul>
 *
 * <p>No new guarantee is added. In particular the handle still decides catch up and clamping; this
 * class only sorts by the value the handle wrote.
 */
public final class KairosScheduler extends Scheduler {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Kairos");

    /**
     * Share of the cores the whole server aims to stay below, the same rule the compute budget uses.
     * The default ceiling is derived from it when the operator did not pick a number.
     */
    private static final double USABLE_CORE_FRACTION = 0.8d;

    /**
     * Minimum time between two retires, so a burst that ends cannot shrink the fund faster than one
     * thread per second. A sawtooth of a few hundred milliseconds therefore cannot happen at all.
     */
    private static final long RETIRE_COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(1L);

    /**
     * How often a failing supervisor pass repeats its warning. The fund keeps working, the log must
     * not turn one broken tick deadline into a flood.
     */
    private static final long ERROR_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(60L);

    private static final String SUPERVISOR_NAME = "Tuff Kairos Supervisor";

    /**
     * How long a tick may sit in the wait queue before R5 starts caring when the fund has no period
     * sample yet. Two periods at 20 TPS: a tick that is a whole tick late is starvation, a tick that is
     * a few hundred microseconds late is noise. It is also the floor of the derived threshold, so a
     * healthy 20 TPS server keeps exactly this number.
     */
    private static final long OVERDUE_FLOOR_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    /**
     * The period of a healthy server, 20 ticks per second. It seeds the observed period so the first
     * pass of the detector uses the documented floor instead of a wild guess.
     */
    private static final long NOMINAL_PERIOD_NANOS = TimeUnit.MILLISECONDS.toNanos(50L);

    /**
     * Ceiling for a period sample. A region that was inactive for a minute, or a clock jump, must not
     * move the overdue threshold for the whole server by that amount.
     */
    private static final long PERIOD_SAMPLE_CAP_NANOS = TimeUnit.SECONDS.toNanos(10L);

    /**
     * Longest a parked runner sleeps without rechecking the halt flag. Halt unparks every runner, so
     * this only matters in the tiny window where a runner decided to park and the unpark arrived
     * first; without it that window would turn into a thread that outlives the join budget.
     */
    private static final long IDLE_PARK_NANOS = TimeUnit.SECONDS.toNanos(1L);

    /**
     * At most one overdue warning per this window. The detector still counts the suppressed passes so
     * the next warning can say how many it swallowed; without the quiet window a starving server would
     * write a line per second and bury the reason.
     */
    private static final long OVERDUE_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10L);

    /**
     * The scheduler that is actually running, so the console can report the fund without a server
     * reference. Set by {@link #start()} and cleared by {@link #halt()}.
     */
    private static final AtomicReference<@Nullable KairosScheduler> ACTIVE = new AtomicReference<>();

    private final ToLongFunction<SchedulableTick> scheduledStart;
    private final ThreadFactory threadFactory;

    /**
     * The handle questions catch-up needs. {@link KairosTickControl#none()} when the caller has no
     * region handle to offer, in which case catch-up quietly does not engage.
     */
    private final KairosTickControl ticks;

    /** Deadline order, shared by the global queue and by every runner backlog. */
    private final Comparator<ScheduledState> byDeadline;

    /**
     * Monotonic time used by the supervisor, the stale-tick detector and the snapshot. Production
     * always reads {@link System#nanoTime()}; tests supply a clock they can move.
     */
    private final LongSupplier clock;

    private final LinkedSortedSet<ScheduledState> awaiting;
    private final PriorityQueue<ScheduledState> queued;

    /** Permit bits, indexed by runner slot. Guarded by {@link #scheduleLock}. */
    private final BitSet idleThreads;

    /**
     * Ownership of a tick while it is scheduled here. The map replaces the state field leafpile keeps
     * inside {@link SchedulableTick}; entries are dropped as soon as the tick stops rescheduling.
     */
    private final Map<SchedulableTick, ScheduledState> states = new IdentityHashMap<>();

    private final Object scheduleLock = new Object();
    private final Object lifecycleLock = new Object();

    /**
     * Append only while the fund is not full. A dead slot is reused by the next spawn, so the array
     * never grows past {@code max} and slot indices stay valid. Published under {@link #lifecycleLock}.
     */
    private volatile Runner[] slots;

    /** Every runner thread that was ever created, in creation order. The join contract needs all of them. */
    private final List<Thread> everCreated = new CopyOnWriteArrayList<>();

    /** Started and not yet exited. Only the supervisor and the runner loop touch it. */
    private final AtomicInteger live = new AtomicInteger();
    private final AtomicLong spawnCount = new AtomicLong();
    private final AtomicLong retireCount = new AtomicLong();

    /**
     * Highest {@code live + queued} seen since the last supervisor pass. A tick only reaches the queue
     * when every permit is taken, so this is the demand that the current fund could not serve. Read
     * and reset by the supervisor under {@link #scheduleLock}.
     */
    private int peakDemand;

    private volatile @Nullable Thread supervisor;

    /** Fund size the last supervisor pass asked for. Read by the snapshot; writer is the supervisor. */
    private volatile int targetThreads;

    /** Where the resolved ceiling came from, printed by the command and the startup line. */
    private volatile String budgetSource = "not started";

    /** What the last supervisor pass did, with the numbers behind it. */
    private volatile FundDecision lastDecision;

    /** Supervisor only: not before this instant may the next overdue warning be written. */
    private long lastOverdueWarnNanos;

    /** Supervisor only: overdue passes that the quiet window swallowed since the last warning. */
    private long suppressedOverdueWarns;

    private volatile int minThreads;

    /** Equal to {@code minThreads} until the tuning is known; then the resolved ceiling. */
    private volatile int maxThreads;

    private volatile boolean elastic;
    private volatile KairosTuning tuning = KairosTuning.fixed();

    /** Tick level knobs, resolved at start. Defaults keep the stock queue. */
    private volatile KairosKernel kernel = KairosKernel.defaults();

    /** Whether the per-tick and per-pass recording is armed. Read once per body, never allocates. */
    private volatile boolean jfr;

    /** Ticks a runner may keep for itself, {@code 0} when soft affinity is off. */
    private volatile int affinityQueueSize;

    /** How late a kept tick must be before another runner may take it. */
    private volatile long stealThresholdNanos;

    /**
     * Average distance between two bodies of the same handle, in nanoseconds. Runners update it; the
     * detector reads it. A best effort sample: a lost update only means one more pass at the previous
     * average, which is what a metric of this kind is for.
     */
    private volatile long observedPeriodNanos = NOMINAL_PERIOD_NANOS;

    /** Whether {@link #observedPeriodNanos} already saw a sample; the first one replaces the seed. */
    private volatile boolean periodSampled;

    /** Ticks sitting in runner backlogs. Guarded by {@link #scheduleLock}. */
    private int localDepth;

    private volatile boolean started;
    private volatile boolean halted;

    /** Ticks that were handed back to the runner that just ran them. */
    private final LongAdder affinityHits = new LongAdder();

    /** Ticks another runner took out of a backlog because they were late. */
    private final LongAdder stolenTicks = new LongAdder();

    /** Permits whose wait was measured. */
    private final LongAdder leaseGrants = new LongAdder();

    /** Permits whose wait exceeded the configured lease timeout. */
    private final LongAdder leaseTimeouts = new LongAdder();

    /** Longest wait between a tick deadline and its permit grant, best effort. */
    private volatile long leaseQueuedMaxNanos;

    /** Longest wait between a permit grant and the tick start, best effort. */
    private volatile long leaseParkedMaxNanos;

    /** Set by a runner that saw a wait above the timeout; cleared by the warning that reports it. */
    private volatile boolean leaseTimeoutPending;

    /** Catch-up bursts the fund started. */
    private final LongAdder catchUpBursts = new LongAdder();

    /** Extra tick bodies run to recover a backlog beyond the first body of a burst. */
    private final LongAdder catchUpTicks = new LongAdder();

    /** Supervisor only: not before this instant may the next lease warning be written. */
    private long lastLeaseWarnNanos;

    /** Supervisor only: lease timeouts the quiet window swallowed since the last warning. */
    private long suppressedLeaseWarns;

    /** Supervisor only: at least one lease warning was written, so the quiet window has a start. */
    private boolean leaseWarned;

    /**
     * Not before this instant may the next runner be retired. Supervisor only.
     */
    private long nextRetireNanos;

    /**
     * When the floor last moved. A deliberate floor change shortens the keep alive, so a live resize
     * lands in seconds instead of waiting out the idle window. Supervisor only.
     */
    private volatile long floorChangedNanos;

    /**
     * Spawn tokens, refilled by elapsed time at {@code spawn-rate-per-second}. Supervisor only.
     */
    private double spawnTokens;
    private long spawnTokensUpdatedNanos;

    /**
     * Creates the {@code min} runners of the fund. Nothing runs until {@link #start()}.
     *
     * @param threads        initial floor of the fund; the ceiling comes from the tuning at start
     * @param threadFactory  factory owned by the caller; it decides the thread class, group and handler
     * @param scheduledStart reader for a tick's current deadline, in nanoseconds
     * @throws IllegalArgumentException if {@code threads} is not positive
     * @throws NullPointerException     if the factory or the reader is {@code null}
     */
    public KairosScheduler(
        final int threads,
        final ThreadFactory threadFactory,
        final ToLongFunction<SchedulableTick> scheduledStart
    ) {
        this(threads, threadFactory, scheduledStart, KairosTickControl.none(), System::nanoTime);
    }

    /**
     * Creates the fund with the handle questions catch-up needs.
     *
     * @param threads        initial floor of the fund
     * @param threadFactory  factory owned by the caller
     * @param scheduledStart reader for a tick's current deadline, in nanoseconds
     * @param ticks          source of the missed period count and of the write-off limit
     * @throws IllegalArgumentException if {@code threads} is not positive
     * @throws NullPointerException     if an argument is {@code null}
     */
    public KairosScheduler(
        final int threads,
        final ThreadFactory threadFactory,
        final ToLongFunction<SchedulableTick> scheduledStart,
        final KairosTickControl ticks
    ) {
        this(threads, threadFactory, scheduledStart, ticks, System::nanoTime);
    }

    /**
     * Creates the fund with a clock the tests can move.
     *
     * <p>The clock drives the supervisor, the stale-tick detector, the snapshot and the observed
     * period. Parking a runner still uses real time, because a deadline comes from Folia's handle and
     * must not be reinterpreted through a fake clock.
     *
     * @param threads        initial floor of the fund
     * @param threadFactory  factory owned by the caller
     * @param scheduledStart reader for a tick's current deadline, in nanoseconds
     * @param ticks          source of the missed period count and of the write-off limit
     * @param clock          monotonic time source, never {@code null}
     * @throws IllegalArgumentException if {@code threads} is not positive
     */
    KairosScheduler(
        final int threads,
        final ThreadFactory threadFactory,
        final ToLongFunction<SchedulableTick> scheduledStart,
        final KairosTickControl ticks,
        final LongSupplier clock
    ) {
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be positive, got " + threads);
        }
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        this.scheduledStart = Objects.requireNonNull(scheduledStart, "scheduledStart");
        this.ticks = Objects.requireNonNull(ticks, "ticks");
        this.clock = Objects.requireNonNull(clock, "clock");

        final Comparator<ScheduledState> byDeadline = (final ScheduledState s1, final ScheduledState s2) -> {
            final SchedulableTick t1 = s1.tick;
            final SchedulableTick t2 = s2.tick;

            final int timeCompare = TimeUtil.compareTimes(
                this.scheduledStart.applyAsLong(t1), this.scheduledStart.applyAsLong(t2)
            );
            if (timeCompare != 0) {
                return timeCompare;
            }

            // the tie break is part of the contract; do not "improve" it
            return Long.signum(t1.id - t2.id);
        };
        this.byDeadline = byDeadline;
        this.awaiting = new LinkedSortedSet<>(byDeadline);
        this.queued = new PriorityQueue<>(byDeadline);

        final Runner[] initial = new Runner[threads];
        for (int slot = 0; slot < threads; ++slot) {
            initial[slot] = this.createRunner(slot);
        }
        this.slots = initial;

        final BitSet idle = new BitSet(threads);
        idle.set(0, threads);
        this.idleThreads = idle;

        this.minThreads = threads;
        this.maxThreads = threads;
        this.targetThreads = threads;
        this.lastDecision = new FundDecision(KairosSnapshot.Decision.IDLE, threads, threads, threads);
    }

    /**
     * Starts every runner of the floor, then the supervisor when the fund may grow.
     *
     * <p>The gate and the fund knobs live here rather than in the constructor because the constructor
     * runs while {@code paper-global.yml} is being loaded, before {@code config/tuff.yml} is read.
     *
     * @throws IllegalStateException if the operator did not opt in, the scheduler was halted, or it
     *                               was already started
     */
    public void start() {
        KairosGate.requireOptIn();
        final KairosTuning fundTuning = KairosGate.tuning();
        final KairosServices services = KairosGate.services();
        synchronized (this.lifecycleLock) {
            if (this.halted) {
                throw new IllegalStateException("Kairos was halted before it started");
            }
            if (this.started) {
                throw new IllegalStateException("Kairos was already started");
            }
            this.tuning = fundTuning;
            this.elastic = fundTuning.elastic();
            this.kernel = KairosGate.kernel();
            this.jfr = this.kernel.jfr();
            this.affinityQueueSize = this.kernel.affinityQueueSize();
            this.stealThresholdNanos = TimeUnit.MILLISECONDS.toNanos(this.kernel.stealThresholdMillis());
            final int floor = this.minThreads;
            // one source for the ceiling: the thread layer's controller when it runs, the documented
            // core fraction otherwise. the port already accounts for the reserve, so nothing here
            // subtracts it again
            this.maxThreads = this.elastic
                ? this.resolveMax(floor, fundTuning.maxThreads(), services.budget())
                : floor;
            if (!this.elastic) {
                this.budgetSource = "fixed fund (elasticity is off)";
            }
            this.targetThreads = floor;
            this.started = true;
            this.floorChangedNanos = this.clock.getAsLong();

            for (final Runner runner : this.slots) {
                runner.prepareBacklog();
                runner.thread.start();
                this.live.incrementAndGet();
            }

            if (this.elastic && this.maxThreads > floor) {
                final Thread.UncaughtExceptionHandler failureHandler =
                    (final Thread thread, final Throwable failure) ->
                        LOGGER.error("The Kairos supervisor died; the fund keeps its current size", failure);
                Thread created = services.supervisors().create(SUPERVISOR_NAME, this::supervisorLoop, failureHandler);
                if (created == null) {
                    LOGGER.info(
                        "Kairos: the supervisor is a plain daemon because {}; thread dumps show it by name",
                        services.supervisors().describe()
                    );
                    created = new Thread(this::supervisorLoop, SUPERVISOR_NAME);
                    created.setDaemon(true);
                    created.setUncaughtExceptionHandler(failureHandler);
                }
                this.supervisor = created;
                created.start();
            }
            ACTIVE.set(this);
            this.logStart(floor);
        }
    }

    /**
     * Moves the floor of the fund, clamped to what the fund can do, without recreating a live runner.
     *
     * <p>With elasticity on this is a real live resize: the supervisor grows the fund towards the new
     * floor and retires the surplus above it. With elasticity off the fund is the fixed prototype and
     * the only honest answer is a warning that a restart is needed.
     *
     * @param threads requested tick thread count, at least 1
     */
    public void setThreads(final int threads) {
        if (threads < 1) {
            LOGGER.warn("Kairos: threaded-regions.threads is {}, which is not a thread count; ignored", threads);
            return;
        }
        final boolean raise;
        synchronized (this.lifecycleLock) {
            if (!this.started) {
                synchronized (this.scheduleLock) {
                    this.resizeBeforeStart(threads);
                }
                return;
            }
            if (!this.elastic) {
                LOGGER.warn(
                    "Kairos: threaded-regions.threads is {} but the fund is fixed at {} (elasticity is off); "
                        + "changing it needs a restart",
                    threads,
                    this.minThreads
                );
                return;
            }
            raise = threads > this.minThreads;
            final int previousMax = this.maxThreads;
            this.minThreads = threads;
            this.floorChangedNanos = this.clock.getAsLong();
            if (this.maxThreads < threads) {
                this.maxThreads = threads;
                LOGGER.warn(
                    "Kairos: threaded-regions.threads {} is above the ceiling {}; the ceiling is raised to {}",
                    threads,
                    previousMax,
                    threads
                );
            }
        }
        if (raise) {
            // a lowered ceiling is drained by the regular pass, a raised floor should not wait a second
            LockSupport.unpark(this.supervisor);
        }
        LOGGER.info("Kairos: floor is now {} thread(s) (max {}); no live thread is recreated", threads, this.maxThreads);
    }

    /**
     * Returns the floor of the fund.
     *
     * @return the number of runners that always exist while the fund runs
     */
    public int minThreads() {
        return this.minThreads;
    }

    /**
     * Returns the ceiling of the fund after the tuning was applied.
     *
     * @return the most runners the fund may ever have
     */
    public int maxThreads() {
        return this.maxThreads;
    }

    /**
     * Returns how many runners are started and have not exited yet.
     *
     * @return the live size of the fund
     */
    public int liveThreads() {
        return this.live.get();
    }

    /**
     * Returns whether the fund is allowed to change size.
     *
     * @return {@code true} when the supervisor runs
     */
    public boolean elastic() {
        return this.elastic;
    }

    /**
     * Returns how many runners the supervisor started since the process began.
     *
     * @return the spawn counter
     */
    public long spawnCount() {
        return this.spawnCount.get();
    }

    /**
     * Returns how many runners the supervisor retired since the process began.
     *
     * @return the retire counter
     */
    public long retireCount() {
        return this.retireCount.get();
    }

    /**
     * Takes a snapshot of the active scheduler.
     *
     * <p>The command path calls this; it is safe on any thread and cheap enough for a tick thread: it
     * reads counters and walks the two tick queues under the schedule lock, never a region.
     *
     * @return the snapshot, or {@code null} when Kairos never started or was halted
     */
    public static @Nullable KairosSnapshot activeSnapshot() {
        final KairosScheduler current = ACTIVE.get();
        return current == null ? null : current.snapshot();
    }

    /**
     * Takes one consistent snapshot of this fund.
     *
     * <p>The queue walk happens under {@link #scheduleLock}; the queues hold at most one entry per
     * region, so the pass is small even on a loaded server. The method never reads a tick body and
     * never waits for a runner.
     *
     * @return the snapshot, never {@code null}
     */
    public KairosSnapshot snapshot() {
        final long now = this.clock.getAsLong();
        final long threshold = this.overdueThresholdNanos();
        int awaiting = 0;
        int executing = 0;
        int queued = 0;
        int overdue = 0;
        long oldestOverdue = 0L;
        long totalAge = 0L;
        long longestAge = 0L;
        int aged = 0;
        synchronized (this.scheduleLock) {
            for (final Runner runner : this.slots) {
                if (runner == null || !runner.thread.isAlive()) {
                    continue;
                }
                final int state = runner.state.state;
                if (state == Runner.STATE_AWAITING_TICK) {
                    ++awaiting;
                } else if (state == Runner.STATE_EXECUTING_TICK) {
                    ++executing;
                }
            }
            for (final ScheduledState state : this.queued) {
                final long age = this.waitAge(now, state);
                ++queued;
                if (age > 0L) {
                    totalAge += age;
                    longestAge = Math.max(longestAge, age);
                    ++aged;
                }
                if (age >= threshold) {
                    ++overdue;
                    oldestOverdue = Math.max(oldestOverdue, age);
                }
            }
            for (final Runner runner : this.slots) {
                if (runner == null || runner.localBacklog == null) {
                    continue;
                }
                for (final ScheduledState state : runner.localBacklog) {
                    // a kept tick is still waiting for a permit; leaving it out would make the queue
                    // look shorter and the detector blind exactly when affinity is on
                    final long age = this.waitAge(now, state);
                    ++queued;
                    if (age > 0L) {
                        totalAge += age;
                        longestAge = Math.max(longestAge, age);
                        ++aged;
                    }
                    if (age >= threshold) {
                        ++overdue;
                        oldestOverdue = Math.max(oldestOverdue, age);
                    }
                }
            }
            for (final ScheduledState state : this.awaiting) {
                final long age = this.waitAge(now, state);
                if (age > 0L) {
                    totalAge += age;
                    longestAge = Math.max(longestAge, age);
                    ++aged;
                }
            }
        }
        final double averageMillis = aged == 0 ? 0.0d : totalAge / (double) aged / 1_000_000.0d;
        return new KairosSnapshot(
            this.elastic,
            this.minThreads,
            this.maxThreads,
            this.live.get(),
            this.targetThreads,
            queued,
            awaiting,
            executing,
            overdue,
            TimeUnit.NANOSECONDS.toMillis(oldestOverdue),
            averageMillis,
            longestAge / 1_000_000.0d,
            TimeUnit.NANOSECONDS.toMillis(threshold),
            this.supervisorState(),
            this.lastDecision.kind(),
            this.lastDecision.describe(),
            this.spawnCount.get(),
            this.retireCount.get(),
            this.budgetSource,
            this.degradedReasons(overdue),
            new KairosSnapshot.LeaseWait(
                this.leaseGrants.sum(),
                TimeUnit.NANOSECONDS.toMillis(this.leaseQueuedMaxNanos),
                TimeUnit.NANOSECONDS.toMillis(this.leaseParkedMaxNanos),
                this.leaseTimeouts.sum(),
                this.kernel.leaseTimeoutMillis()
            ),
            this.affinityHits.sum(),
            this.stolenTicks.sum(),
            this.catchUpBursts.sum(),
            this.catchUpTicks.sum()
        );
    }

    /**
     * Returns the wait above which a queued tick counts as overdue.
     *
     * <p>Two observed periods, floored at the 20 TPS value so a healthy server keeps the documented
     * number. The constant this replaces was blind at low TPS: at 12 TPS a whole period is 83 ms, so a
     * tick that had already missed its slot never reached 100 ms and the detector stayed silent.
     *
     * @return the threshold in nanoseconds
     */
    long overdueThresholdNanos() {
        final long derived = 2L * this.observedPeriodNanos;
        return Math.max(OVERDUE_FLOOR_NANOS, derived);
    }

    private KairosSnapshot.SupervisorState supervisorState() {
        if (!this.elastic) {
            return KairosSnapshot.SupervisorState.OFF;
        }
        if (!this.started) {
            return KairosSnapshot.SupervisorState.NOT_STARTED;
        }
        if (this.halted) {
            return KairosSnapshot.SupervisorState.STOPPED;
        }
        final Thread created = this.supervisor;
        if (created == null) {
            // no ceiling above the floor is the only legal reason to run without one
            return this.maxThreads <= this.minThreads
                ? KairosSnapshot.SupervisorState.OFF
                : KairosSnapshot.SupervisorState.NOT_STARTED;
        }
        return created.isAlive() ? KairosSnapshot.SupervisorState.RUNNING : KairosSnapshot.SupervisorState.DEAD;
    }

    private List<KairosSnapshot.Degraded> degradedReasons(final int overdue) {
        final List<KairosSnapshot.Degraded> reasons = new ArrayList<>(3);
        if (this.elastic && this.maxThreads <= this.minThreads) {
            reasons.add(KairosSnapshot.Degraded.NO_HEADROOM);
        }
        if (this.elastic && this.started && !this.halted) {
            final Thread created = this.supervisor;
            if (created != null && !created.isAlive()) {
                reasons.add(KairosSnapshot.Degraded.SUPERVISOR_DEAD);
            }
        }
        if (overdue > 0) {
            reasons.add(KairosSnapshot.Degraded.QUEUED_OVERDUE);
        }
        return List.copyOf(reasons);
    }

    private long waitAge(final long now, final ScheduledState state) {
        final long start = this.scheduledStart.applyAsLong(state.tick);
        if (start == TimeUtil.DEADLINE_NOT_SET || start <= 0L) {
            return 0L;
        }
        final long age = now - start;
        return Math.max(0L, age);
    }

    /**
     * Returns the live threads of the fund, including any that are still finishing a tick.
     *
     * @return a fresh array, never {@code null}
     */
    @Override
    public Thread[] getAliveThreads() {
        final List<Thread> ret = new ArrayList<>(this.everCreated.size());
        for (final Thread thread : this.everCreated) {
            if (thread.isAlive()) {
                ret.add(thread);
            }
        }
        return ret.toArray(new Thread[0]);
    }

    /**
     * Returns the threads currently allocated for executing ticks: live and not retiring.
     *
     * @return a fresh array, never {@code null}
     */
    @Override
    public Thread[] getCoreThreads() {
        final List<Thread> ret = new ArrayList<>(this.slots.length);
        for (final Runner runner : this.slots) {
            if (runner != null && runner.thread.isAlive() && runner.state.state != Runner.STATE_RETIRING) {
                ret.add(runner.thread);
            }
        }
        return ret.toArray(new Thread[0]);
    }

    /**
     * Stops the supervisor and every runner at their next loop check, without interrupting a running
     * tick.
     *
     * <p>Idempotent, never blocks and is safe from a tick thread, which is what the shutdown path
     * needs. {@link #join(long)} then waits for the supervisor and for every thread ever created.
     */
    @Override
    public void halt() {
        this.halted = true;
        ACTIVE.compareAndSet(this, null);
        for (final Thread thread : this.everCreated) {
            // force a response even when the runner is parked
            LockSupport.unpark(thread);
        }
        final Thread created = this.supervisor;
        if (created != null) {
            LockSupport.unpark(created);
        }
    }

    /**
     * Waits for the supervisor and for every thread ever created, swallowing interrupts like the EDF
     * original.
     *
     * @param msToWait maximum wait, or {@code 0} for no limit
     * @return {@code false} if the deadline passed first
     */
    @Override
    public boolean join(final long msToWait) {
        try {
            return this.join(msToWait, false);
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Waits for the supervisor and for every thread ever created and propagates an interrupt.
     *
     * @param msToWait maximum wait, or {@code 0} for no limit
     * @return {@code false} if the deadline passed first
     * @throws InterruptedException if this thread is interrupted
     */
    @Override
    public boolean joinInterruptable(final long msToWait) throws InterruptedException {
        return this.join(msToWait, true);
    }

    private boolean join(final long msToWait, final boolean interruptable) throws InterruptedException {
        final boolean timed = msToWait > 0L;
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(msToWait);
        // a halt can race with one last spawn, so re-read the list until it stops growing
        int seen = -1;
        while (seen != this.everCreated.size()) {
            seen = this.everCreated.size();
            if (!joinEach(this.everCreated, deadline, timed, interruptable)) {
                return false;
            }
            final Thread created = this.supervisor;
            if (created != null && !joinEach(List.of(created), deadline, timed, interruptable)) {
                return false;
            }
        }
        return true;
    }

    private static boolean joinEach(
        final List<Thread> threads,
        final long deadline,
        final boolean timed,
        final boolean interruptable
    ) throws InterruptedException {
        boolean interrupted = false;
        try {
            for (final Thread thread : threads) {
                while (thread.isAlive()) {
                    try {
                        if (timed) {
                            final long current = System.nanoTime();
                            if (current - deadline >= 0L) {
                                return false;
                            }
                            thread.join(Duration.ofNanos(deadline - current));
                        } else {
                            thread.join();
                        }
                    } catch (final InterruptedException ex) {
                        if (interruptable) {
                            throw ex;
                        }
                        interrupted = true;
                    }
                }
            }
            return true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Schedules a tick by its deadline.
     *
     * <p>Runs on the caller's thread, takes only the schedule lock and never creates or starts a
     * thread. The tick must already carry a start value, exactly as Folia's handle sets it.
     *
     * @param tick tick to schedule
     * @throws IllegalStateException if this tick is already owned by this scheduler
     */
    @Override
    public void schedule(final SchedulableTick tick) {
        final boolean queuedNow;
        synchronized (this.scheduleLock) {
            if (this.states.containsKey(tick)) {
                throw new IllegalStateException("Task " + tick + " is already scheduled");
            }
            final ScheduledState state = new ScheduledState(tick);
            this.states.put(tick, state);
            state.schedulerOwnedBy = this;
            queuedNow = this.insertFresh(state);
        }
        final Thread created = this.supervisor;
        if (queuedNow && created != null && this.live.get() < this.maxThreads) {
            // demand signal only, the supervisor is the one that spawns
            LockSupport.unpark(created);
        }
    }

    /**
     * Keeps the EDF contract: intermediate task execution is not supported.
     *
     * @param tick notified tick; ignored
     */
    @Override
    public void notifyTasks(final SchedulableTick tick) {
        // Not implemented, same as EDF
    }

    /**
     * Cancels a tick that is still waiting, if it can be removed without racing execution.
     *
     * <p>Best effort, never throws. A tick that is already executing is not cancellable, exactly as
     * in EDF; Folia itself never calls this, it only marks handles non schedulable.
     *
     * @param tick tick to cancel
     * @return {@code true} if this call removed the tick
     */
    @Override
    public boolean cancel(final SchedulableTick tick) {
        final ScheduledState state;
        synchronized (this.scheduleLock) {
            state = this.states.get(tick);
            if (state == null || state.schedulerOwnedBy != this) {
                return false;
            }
            if (this.queued.remove(state)) {
                return true;
            }
            if (state.localOwner != null) {
                // a tick kept by a runner is still unleased, so it is cancellable just like a queued one
                return this.unstash(state.localOwner, state);
            }
            if (state.awaitingLink != null) {
                this.awaiting.remove(state.awaitingLink);
                state.awaitingLink = null;

                final Runner runner = state.ownedBy;
                state.ownedBy = null;
                // the permit has to be handed over through the same transition a returning runner
                // uses, otherwise the replacement is owned and running but invisible to the queues,
                // and the next tick start would throw on the runner thread
                final ScheduledState replace = this.pollNext(runner);
                if (replace == null) {
                    this.idleThreads.set(runner.slot);
                    runner.forceIdle();
                } else {
                    this.grant(runner, replace);
                }
                return true;
            }
            return false;
        }
    }

    /**
     * Offers a tick to a free permit, the worst waiting tick, or the queue, in that order.
     *
     * @param task tick to place
     * @return {@code true} when the tick went to the queue instead of a free permit
     */
    private boolean insertFresh(final ScheduledState task) {
        final Runner[] runners = this.slots;

        final int firstIdleThread = this.idleThreads.nextSetBit(0);
        if (firstIdleThread != -1) {
            this.grant(runners[firstIdleThread], task);
            return false;
        }

        // try to replace the last awaiting task, the same eviction EDF performs
        final ScheduledState last = this.awaiting.last();
        if (last != null && this.compareByDeadline(task, last) < 0) {
            this.awaiting.pollLast();
            last.awaitingLink = null;
            final Runner runner = last.ownedBy;
            last.ownedBy = null;
            this.queued.add(last);

            this.grant(runner, task);
            this.recordDemand();
            return false;
        }

        this.queued.add(task);
        this.recordDemand();
        return true;
    }

    /**
     * The one place that hands a tick to a runner.
     *
     * <p>Four paths can move a tick into a permit - a fresh schedule, a spawn that fills the fund, a
     * runner returning from a tick, and a cancel that has to replace the tick it removed - and all of
     * them have to leave the same state behind: the tick linked into {@code awaiting}, marked as owned
     * by the runner, stamped with the grant, and the runner's own state pointing at it. Anything less
     * makes the tick invisible to the queue counters and turns the next {@link #takeTask} into an
     * exception on a runner thread, which is outside Folia's handler and would kill the fund silently.
     *
     * <p>The caller has already removed the tick from whatever queue held it and cleared the previous
     * owner; this method does not search the queues. Guarded by {@link #scheduleLock}.
     *
     * @param runner runner that is about to hold the permit
     * @param task   unowned, unlinked tick to hand over
     * @throws IllegalStateException if the tick is still attached to a queue, an owner, or a runner in
     *                               a state that cannot take a permit
     */
    private void grant(final Runner runner, final ScheduledState task) {
        if (task.ownedBy != null || task.awaitingLink != null || task.localOwner != null) {
            throw new IllegalStateException("Task is already scheduled: " + task.tick);
        }
        final int state = runner.state.state;
        if (state != Runner.STATE_IDLE && state != Runner.STATE_AWAITING_TICK
            && state != Runner.STATE_EXECUTING_TICK) {
            throw new IllegalStateException("Cannot hand a tick to a runner in state " + state);
        }
        if (state == Runner.STATE_IDLE) {
            this.idleThreads.clear(runner.slot);
        }
        task.ownedBy = runner;
        task.grantedNanos = this.clock.getAsLong();
        task.awaitingLink = this.awaiting.addLast(task);
        runner.setStateVolatile(new Runner.TickThreadRunnerState(task, Runner.STATE_AWAITING_TICK));
        LockSupport.unpark(runner.getRunnerThread());
    }

    /**
     * Remembers that a tick had to wait, so the supervisor sees the burst even when it samples a quiet
     * moment. Guarded by {@link #scheduleLock}.
     */
    private void recordDemand() {
        this.peakDemand = Math.max(this.peakDemand, this.live.get() + this.queued.size() + this.localDepth);
    }

    private int compareByDeadline(final ScheduledState s1, final ScheduledState s2) {
        final SchedulableTick t1 = s1.tick;
        final SchedulableTick t2 = s2.tick;
        final int timeCompare = TimeUtil.compareTimes(
            this.scheduledStart.applyAsLong(t1), this.scheduledStart.applyAsLong(t2)
        );
        if (timeCompare != 0) {
            return timeCompare;
        }
        return Long.signum(t1.id - t2.id);
    }

    private void takeTask(final Runner runner, final ScheduledState tick) {
        if (!this.awaiting.remove(tick.awaitingLink)) {
            throw new IllegalStateException("Task is not in awaiting");
        }
        tick.awaitingLink = null;
        this.recordWait(tick);
    }

    /**
     * Records how long the permit took, and the period between two bodies of the same handle.
     *
     * <p>Two waits are kept apart because they name different faults. The queued wait is the distance
     * between the deadline and the grant: a region that had to wait for a permit, which is the signal
     * that the fund is short. The parked wait is the distance between the later of the grant and the
     * deadline and the actual start: the permit existed and the tick was due, yet the body still did
     * not run, which is CPU or parking contention rather than a shortage of permits.
     *
     * <p>Cheap on purpose: a few counter updates on the runner thread. Nothing here locks, logs or
     * allocates, because this is the tick path; the warning that reports a timeout is written by the
     * supervisor.
     *
     * @param tick tick whose body is about to run
     */
    private void recordWait(final ScheduledState tick) {
        final long now = this.clock.getAsLong();
        final long granted = tick.grantedNanos;
        if (granted != 0L) {
            final long deadline = this.scheduledStart.applyAsLong(tick.tick);
            final long queued = deadline == TimeUtil.DEADLINE_NOT_SET ? 0L : Math.max(0L, granted - deadline);
            final long reference = deadline == TimeUtil.DEADLINE_NOT_SET ? granted : Math.max(granted, deadline);
            final long parked = Math.max(0L, now - reference);
            this.leaseGrants.increment();
            if (queued > this.leaseQueuedMaxNanos) {
                this.leaseQueuedMaxNanos = queued;
            }
            if (parked > this.leaseParkedMaxNanos) {
                this.leaseParkedMaxNanos = parked;
            }
            final long timeout = TimeUnit.MILLISECONDS.toNanos(this.kernel.leaseTimeoutMillis());
            if (timeout > 0L && Math.max(queued, parked) >= timeout) {
                this.leaseTimeouts.increment();
                this.leaseTimeoutPending = true;
            }
        }
        this.recordPeriod(now, tick);
    }

    /**
     * Feeds the observed period, which the overdue detector derives its threshold from.
     *
     * <p>The sample is the distance between two bodies of the same handle, so a region that is ticked
     * at 12 TPS contributes 83 ms no matter how many regions share the fund. The first sample replaces
     * the 20 TPS seed, which makes the detector honest as soon as a region has ticked twice; later
     * samples are averaged so one freak tick does not move the threshold for the whole server.
     *
     * @param now  current clock reading
     * @param tick tick whose body is about to run
     */
    private void recordPeriod(final long now, final ScheduledState tick) {
        final long previous = tick.lastStartNanos;
        tick.lastStartNanos = now;
        if (previous == 0L) {
            return;
        }
        final long sample = Math.clamp(now - previous, NOMINAL_PERIOD_NANOS, PERIOD_SAMPLE_CAP_NANOS);
        if (!this.periodSampled) {
            this.observedPeriodNanos = sample;
            this.periodSampled = true;
            return;
        }
        final long average = this.observedPeriodNanos;
        this.observedPeriodNanos = Math.clamp(
            (average * 3L + sample) / 4L, NOMINAL_PERIOD_NANOS, PERIOD_SAMPLE_CAP_NANOS
        );
    }

    /**
     * Decides what the next body may write off, which is the whole of catch-up.
     *
     * <p>A handle that is on time, or a fund that was not asked for catch-up, takes the stock path and
     * the handle is never touched. A handle that is at least one whole period behind gets a body that
     * accounts for exactly one period: the region stays late, so it is picked up again immediately
     * through the normal queue, and the lost ticks are actually run instead of skipped. After at most
     * {@code catch-up.cap-ticks} bodies, or when the time budget is spent, the next body settles the
     * remaining backlog exactly like the stock body would, so a region that cannot catch up still
     * returns to its normal cadence instead of keeping a permit busy.
     *
     * <p>Every body still goes through its own acquisition, so the one-permit-per-runner invariant,
     * the cancellation check and the watchdog see exactly what they see on the stock path.
     *
     * @param tick tick whose body is about to run
     */
    private void prepareBody(final ScheduledState tick) {
        final KairosKernel knobs = this.kernel;
        tick.catchUpBody = false;
        if (!knobs.catchUp() && !this.jfr) {
            // the stock path: no seam call, no bookkeeping, exactly what the ported scheduler did
            tick.bodyMissedPeriods = -1L;
            return;
        }
        final long now = this.clock.getAsLong();
        final long behind = this.ticks.missedPeriods().applyAsLong(tick.tick);
        tick.bodyMissedPeriods = behind;
        if (!knobs.catchUp()) {
            return;
        }
        if (behind < 2L) {
            // on time: hand the handle back to the stock mode, but only if a burst happened, so an
            // ordinary tick never touches the seam at all
            if (tick.burstBodies != 0) {
                tick.burstBodies = 0;
                this.ticks.writeOffLimit().accept(tick.tick, 0L);
            }
            return;
        }
        if (tick.burstBodies == 0) {
            tick.burstStartNanos = now;
            this.catchUpBursts.increment();
        }
        final boolean settled = tick.burstBodies + 1 >= knobs.catchUpCapTicks()
            || now - tick.burstStartNanos >= TimeUnit.MILLISECONDS.toNanos(knobs.catchUpBudgetMillis());
        if (settled) {
            // the burst is over: write off the rest, which is exactly the stock body
            tick.burstBodies = 0;
            this.ticks.writeOffLimit().accept(tick.tick, 0L);
            return;
        }
        ++tick.burstBodies;
        tick.catchUpBody = true;
        this.catchUpTicks.increment();
        this.ticks.writeOffLimit().accept(tick.tick, 1L);
    }

    /**
     * Records one executed body for a flight recording, if one is armed.
     *
     * <p>Called on the runner thread right after the body returned. The switch is checked before
     * anything else, so a server that does not record does not pay the region lookup, the seam call or
     * the event allocation.
     *
     * @param tick   tick that just ran
     * @param start  when the body started, {@link System#nanoTime()} terms
     * @param end    when the body returned
     */
    private void recordTick(final ScheduledState tick, final long start, final long end) {
        if (!this.jfr) {
            return;
        }
        final long missed = tick.bodyMissedPeriods;
        final long skipped = missed < 0L
            ? -1L
            : (tick.catchUpBody ? 0L : Math.max(0L, missed - 1L));
        KairosTickEvent.emit(
            regionId(tick.tick),
            tick.tick.id,
            this.scheduledStart.applyAsLong(tick.tick),
            start,
            end,
            missed,
            tick.catchUpBody ? 1 : 0,
            skipped
        );
    }

    /**
     * Queues a tick the runner just finished into its own backlog when soft affinity allows it.
     *
     * <p>Two conditions keep this from becoming a latency trap. The backlog must have room, and no
     * other permit may be free: a free permit is exactly what the tick needs, so it must never be left
     * parked while the tick sits in a private queue behind a busy runner. A tick that was just run has
     * a deadline about one period out, so in the normal case it waits for a permit anyway and the
     * affinity only decides whose permit it is.
     *
     * @param runner runner that just finished the tick
     * @param task   tick that wants to be scheduled again
     * @return {@code true} when the tick was kept for this runner
     */
    private boolean stash(final Runner runner, final ScheduledState task) {
        if (this.affinityQueueSize <= 0 || runner.localBacklog == null) {
            return false;
        }
        if (runner.localBacklog.size() >= this.affinityQueueSize) {
            return false;
        }
        if (this.idleThreads.nextSetBit(0) >= 0) {
            return false;
        }
        task.localOwner = runner;
        runner.localBacklog.add(task);
        ++this.localDepth;
        return true;
    }

    private boolean unstash(final Runner runner, final ScheduledState task) {
        if (runner.localBacklog == null || !runner.localBacklog.remove(task)) {
            return false;
        }
        task.localOwner = null;
        --this.localDepth;
        return true;
    }

    /**
     * Picks the next tick for a permit that just became free.
     *
     * <p>Order is: this runner's own kept tick, the global queue, and one candidate kept by another
     * runner. The own backlog comes first on purpose - that is the whole point of affinity - and the
     * two are compared by the EDF comparator before either is taken, so an earlier global tick still
     * wins. A foreign candidate is only taken when it is already past its deadline by at least the
     * steal threshold, and stealing moves an unleased tick, never a permit, which is what keeps the
     * one-permit-per-runner invariant intact.
     *
     * <p>Guarded by {@link #scheduleLock}.
     *
     * @param runner runner the permit belongs to
     * @return the tick to hand over, or {@code null} when nothing is ready
     */
    private @Nullable ScheduledState pollNext(final Runner runner) {
        final long now = this.clock.getAsLong();
        ScheduledState best = runner.localBacklog == null ? null : runner.localBacklog.peek();
        boolean bestIsLocal = best != null;

        final ScheduledState global = this.queued.peek();
        if (global != null && (best == null || this.compareByDeadline(global, best) < 0)) {
            best = global;
            bestIsLocal = false;
        }

        if (this.affinityQueueSize > 0) {
            final Runner victim = this.stealCandidate(runner);
            final ScheduledState kept = victim == null ? null : victim.localBacklog.peek();
            if (kept != null && this.overdueBy(now, kept) >= this.stealThresholdNanos
                && (best == null || this.compareByDeadline(kept, best) < 0)) {
                // the head of the victim backlog is the tick we just peeked at; the lock makes that safe
                victim.localBacklog.poll();
                kept.localOwner = null;
                --this.localDepth;
                this.stolenTicks.increment();
                best = kept;
                bestIsLocal = false;
            }
        }

        if (best == null) {
            return null;
        }
        if (bestIsLocal) {
            runner.localBacklog.poll();
            best.localOwner = null;
            --this.localDepth;
            this.affinityHits.increment();
        } else if (best == global) {
            this.queued.poll();
        }
        return best;
    }

    /**
     * Returns the next runner whose backlog may be inspected for a steal.
     *
     * <p>One candidate per pick, round robin, exactly like the work stealing scheduler Folia ships:
     * with more than a handful of runners the most overdue tick may not be the one inspected, and a
     * steal is an optimisation rather than a promise.
     *
     * @param runner runner that is about to take a tick
     * @return a runner with a backlog, or {@code null}
     */
    private @Nullable Runner stealCandidate(final Runner runner) {
        final Runner[] runners = this.slots;
        if (runners.length < 2) {
            return null;
        }
        for (int step = 1; step < runners.length; ++step) {
            final int index = (runner.slot + step) % runners.length;
            final Runner candidate = runners[index];
            if (candidate == null || candidate == runner || candidate.localBacklog == null
                || candidate.localBacklog.isEmpty()) {
                continue;
            }
            return candidate;
        }
        return null;
    }

    /**
     * Returns how far past its deadline a tick already is.
     *
     * @param now  current clock reading
     * @param task waiting tick
     * @return the positive lateness in nanoseconds, zero when it is not due yet
     */
    private long overdueBy(final long now, final ScheduledState task) {
        final long start = this.scheduledStart.applyAsLong(task.tick);
        if (start == TimeUtil.DEADLINE_NOT_SET) {
            return 0L;
        }
        return Math.max(0L, now - start);
    }

    private @Nullable ScheduledState returnTask(final Runner runner, final @Nullable ScheduledState reschedule) {
        if (reschedule != null && !this.stash(runner, reschedule)) {
            this.queued.add(reschedule);
        }
        final ScheduledState next = this.pollNext(runner);
        if (next == null) {
            this.idleThreads.set(runner.slot);
            return null;
        }
        this.grant(runner, next);
        return next;
    }

    private void release(final ScheduledState task) {
        // the tick will not reschedule, so this scheduler must not keep the handle alive
        this.states.remove(task.tick);
    }

    /**
     * Hands waiting ticks to permits that just became free. Called after a spawn, with the schedule
     * lock held.
     *
     * <p>The pick is the same one a returning runner makes, so a freshly spawned runner may also take
     * a backlog tick that is already late; otherwise it would park next to work it is allowed to do.
     */
    private void pump() {
        int slot;
        while ((slot = this.idleThreads.nextSetBit(0)) >= 0) {
            final Runner runner = this.slots[slot];
            final ScheduledState next = this.pollNext(runner);
            if (next == null) {
                return;
            }
            this.grant(runner, next);
        }
    }

    /**
     * Builds one runner and its thread. Does not start it and does not publish it into {@link #slots};
     * the caller decides where the slot goes.
     *
     * @param slot index the runner will occupy
     * @return the runner and its not yet started thread
     */
    private Runner createRunner(final int slot) {
        final LazyRunnable body = new LazyRunnable();
        final Thread thread = this.threadFactory.newThread(body);
        final Runner runner = new Runner(thread, slot, this);
        body.setRunnable(runner);
        this.everCreated.add(thread);
        return runner;
    }

    /**
     * Applies a new floor before anything was started, so the thread itself can still be dropped.
     * Guarded by both locks; a slot that already holds a permit is never dropped, because a tick that
     * was scheduled before {@code start()} would otherwise be lost.
     *
     * @param threads new floor, at least 1
     */
    private void resizeBeforeStart(final int threads) {
        final Runner[] current = this.slots;
        if (threads > current.length) {
            final Runner[] grown = Arrays.copyOf(current, threads);
            for (int slot = current.length; slot < threads; ++slot) {
                grown[slot] = this.createRunner(slot);
                this.idleThreads.set(slot);
            }
            this.slots = grown;
            this.minThreads = threads;
            this.maxThreads = threads;
            return;
        }
        int keep = threads;
        for (int slot = current.length - 1; slot >= threads; --slot) {
            if (current[slot].state.state != Runner.STATE_IDLE) {
                keep = slot + 1;
                break;
            }
        }
        if (keep < current.length) {
            final Runner[] shrunk = Arrays.copyOf(current, keep);
            for (int slot = current.length - 1; slot >= keep; --slot) {
                this.everCreated.remove(current[slot].thread);
            }
            this.slots = shrunk;
        }
        this.minThreads = keep;
        this.maxThreads = keep;
    }

    /**
     * Adds one runner if the fund has room. Supervisor only, so the fund has exactly one creator.
     *
     * @return {@code true} when a thread was started
     */
    private boolean spawn() {
        synchronized (this.lifecycleLock) {
            if (this.halted) {
                return false;
            }
            if (this.live.get() >= this.maxThreads || this.slots.length >= this.maxThreads) {
                return false;
            }
            final Runner[] before = this.slots;
            int slot = -1;
            for (int index = 0; index < before.length; ++index) {
                final Runner candidate = before[index];
                if (candidate == null || !candidate.thread.isAlive()) {
                    slot = index;
                    break;
                }
            }
            if (slot < 0) {
                slot = before.length;
            }

            // built here rather than through createRunner so a failure to start leaves no trace
            final LazyRunnable body = new LazyRunnable();
            final Thread thread = this.threadFactory.newThread(body);
            final Runner runner = new Runner(thread, slot, this);
            body.setRunnable(runner);
            runner.prepareBacklog();
            final Runner[] grown = slot == before.length ? Arrays.copyOf(before, slot + 1) : before.clone();
            grown[slot] = runner;
            this.slots = grown;
            this.everCreated.add(thread);
            this.live.incrementAndGet();
            try {
                thread.start();
            } catch (final Throwable failure) {
                // roll the half built slot back: the fund keeps its current size, ticks keep running
                this.slots = before;
                this.everCreated.remove(thread);
                this.live.decrementAndGet();
                throw failure;
            }

            this.spawnCount.incrementAndGet();
            synchronized (this.scheduleLock) {
                this.idleThreads.set(slot);
                this.pump();
            }
            if (this.halted) {
                // lost the race with halt: the loop exits at its first check, unpark only speeds it up
                LockSupport.unpark(thread);
            }
            return true;
        }
    }

    /**
     * One fund decision: grow while ticks want permits, retire idle surplus, never both in one pass.
     *
     * <p>After the decision the stale-tick detector runs with the size the pass actually reached, so a
     * warning says "saturated" only when growth was truly unavailable in this pass.
     */
    private void superviseOnce() {
        final long now = this.clock.getAsLong();
        final int target;
        int awaiting = 0;
        int executing = 0;
        synchronized (this.scheduleLock) {
            for (final Runner runner : this.slots) {
                if (runner == null || !runner.thread.isAlive()) {
                    continue;
                }
                final int state = runner.state.state;
                if (state == Runner.STATE_AWAITING_TICK) {
                    ++awaiting;
                } else if (state == Runner.STATE_EXECUTING_TICK) {
                    ++executing;
                }
            }
            final int busy = awaiting + executing;
            // a tick that had to wait means every permit was taken at that moment, so the peak is a
            // better demand signal than a single sample of a queue that drains in microseconds
            final int peak = this.peakDemand;
            this.peakDemand = 0;
            final int demand = Math.max(busy + this.queued.size() + this.localDepth, peak);
            target = Math.max(this.minThreads, Math.min(this.maxThreads, demand));
        }
        this.targetThreads = target;

        final int before = this.live.get();
        boolean grew = false;
        if (before < target && this.refillSpawnTokens(now) && this.spawn()) {
            this.spawnTokens -= 1.0d;
            grew = true;
            // the runner is counted in live immediately, so the intended size is exact
            final int after = before + 1;
            this.recordDecision(KairosSnapshot.Decision.GROW, before, after, target);
            if (after >= this.maxThreads) {
                LOGGER.info("Kairos: fund reached its ceiling of {} tick thread(s) under load", after);
            } else {
                LOGGER.debug("Kairos: fund {} -> {} tick thread(s), target {}", before, after, target);
            }
        }
        if (!grew) {
            final int liveNow = this.live.get();
            if (liveNow > this.minThreads && this.retireOne(target, now)) {
                // the parked runner may need a moment to leave its loop, so the intended size is logged
                final int after = liveNow - 1;
                this.recordDecision(KairosSnapshot.Decision.SHRINK, liveNow, after, target);
                if (after <= this.minThreads) {
                    LOGGER.info("Kairos: fund is back to its floor of {} tick thread(s)", after);
                } else {
                    LOGGER.debug("Kairos: fund {} -> {} tick thread(s), target {}", liveNow, after, target);
                }
            } else {
                this.recordDecision(KairosSnapshot.Decision.IDLE, liveNow, liveNow, target);
            }
        }
        final int overdue = this.warnQueuedOverdue(now);
        this.warnLeaseWait(now);
        if (this.jfr) {
            final int waiting;
            synchronized (this.scheduleLock) {
                waiting = this.queued.size() + this.localDepth;
            }
            // one event per pass: a recording then holds the fund's own timeline next to the per-tick
            // events, which is what explains a hard miss without guessing from a TPS sample
            KairosFundEvent.emit(
                this.minThreads,
                this.live.get(),
                target,
                this.maxThreads,
                waiting,
                awaiting,
                executing,
                overdue,
                this.spawnCount.get(),
                this.retireCount.get()
            );
        }
    }

    /**
     * Keeps the last decision for the command, without allocating a record per idle pass.
     *
     * @param kind   what the pass decided
     * @param before fund size before the decision
     * @param after  intended fund size after it
     * @param target size the demand asked for
     */
    private void recordDecision(
        final KairosSnapshot.Decision kind,
        final int before,
        final int after,
        final int target
    ) {
        final FundDecision previous = this.lastDecision;
        if (kind == KairosSnapshot.Decision.IDLE
            && previous.kind() == KairosSnapshot.Decision.IDLE
            && previous.target() == target) {
            return;
        }
        this.lastDecision = new FundDecision(kind, before, after, target);
    }

    /**
     * Warns about ticks that have been waiting past the threshold, at most once per quiet window.
     *
     * <p>This is risk R5: the Folia watchdog only sees a tick that already started, so a region whose
     * permit never arrives looks like a healthy server to it. The detector separates the two honest
     * cases - the fund has room and is still growing, or the fund is already at its ceiling and the
     * machine cannot help - because the operator's next action is different. Suppressed passes are
     * counted, not logged, and the next warning reports how many it swallowed.
     *
     * @param now supervisor clock at the start of the pass
     * @return how many waiting ticks were already past the threshold
     */
    private int warnQueuedOverdue(final long now) {
        final long threshold = this.overdueThresholdNanos();
        int overdue = 0;
        long oldestAge = 0L;
        long oldestRegion = -1L;
        long oldestTick = -1L;
        synchronized (this.scheduleLock) {
            for (final ScheduledState state : this.queued) {
                final long age = this.waitAge(now, state);
                if (age < threshold) {
                    continue;
                }
                ++overdue;
                if (age > oldestAge) {
                    oldestAge = age;
                    oldestRegion = regionId(state.tick);
                    oldestTick = state.tick.id;
                }
            }
            for (final Runner runner : this.slots) {
                if (runner == null || runner.localBacklog == null) {
                    continue;
                }
                for (final ScheduledState state : runner.localBacklog) {
                    final long age = this.waitAge(now, state);
                    if (age < threshold) {
                        continue;
                    }
                    ++overdue;
                    if (age > oldestAge) {
                        oldestAge = age;
                        oldestRegion = regionId(state.tick);
                        oldestTick = state.tick.id;
                    }
                }
            }
        }
        if (overdue == 0) {
            return 0;
        }
        if (this.lastOverdueWarnNanos != 0L
            && now - this.lastOverdueWarnNanos < OVERDUE_WARN_INTERVAL_NANOS) {
            ++this.suppressedOverdueWarns;
            return overdue;
        }
        final long suppressed = this.suppressedOverdueWarns;
        this.suppressedOverdueWarns = 0L;
        this.lastOverdueWarnNanos = now;

        final int liveNow = this.live.get();
        final boolean saturated = liveNow >= this.maxThreads;
        final long ageMillis = TimeUnit.NANOSECONDS.toMillis(oldestAge);
        final long thresholdMillis = TimeUnit.NANOSECONDS.toMillis(threshold);
        final String oldest = oldestRegion >= 0L
            ? "region " + oldestRegion
            : "region unknown (tick " + oldestTick + ')';
        final String quiet = suppressed == 0L
            ? ""
            : " (" + suppressed + " more overdue pass(es) suppressed since the last warning)";
        if (saturated) {
            LOGGER.warn(
                "Kairos: tick queue is saturated: {} queued tick(s) are past the {} ms wait threshold, "
                    + "oldest {} waited {} ms; the fund is at its ceiling {}/{}{}",
                overdue,
                thresholdMillis,
                oldest,
                ageMillis,
                liveNow,
                this.maxThreads,
                quiet
            );
        } else {
            LOGGER.warn(
                "Kairos: tick starvation: {} queued tick(s) are past the {} ms wait threshold, "
                    + "oldest {} waited {} ms; the fund is at {}/{} thread(s) and can still grow{}",
                overdue,
                thresholdMillis,
                oldest,
                ageMillis,
                liveNow,
                this.maxThreads,
                quiet
            );
        }
        KairosQueuedOverdueEvent.emit(
            oldestRegion, ageMillis, overdue, liveNow, this.maxThreads, saturated
        );
        return overdue;
    }

    /**
     * Warns when a permit was handed out too slowly, at most once per quiet window.
     *
     * <p>The measurement itself happens on the runner thread and is a couple of counters, so no tick
     * ever waits on a lock to record it. This method is the only place that logs, and it runs on the
     * supervisor: a warning from a tick thread would put disk I/O inside somebody's tick.
     *
     * <p>A timeout of zero means accounting only; the snapshot still shows the waits, there is simply
     * no line and no event.
     *
     * @param now supervisor clock at the start of the pass
     */
    private void warnLeaseWait(final long now) {
        if (!this.leaseTimeoutPending) {
            return;
        }
        this.leaseTimeoutPending = false;
        final long threshold = TimeUnit.MILLISECONDS.toNanos(this.kernel.leaseTimeoutMillis());
        if (threshold <= 0L) {
            return;
        }
        if (this.leaseWarned && now - this.lastLeaseWarnNanos < OVERDUE_WARN_INTERVAL_NANOS) {
            ++this.suppressedLeaseWarns;
            return;
        }
        final long suppressed = this.suppressedLeaseWarns;
        this.suppressedLeaseWarns = 0L;
        this.lastLeaseWarnNanos = now;
        this.leaseWarned = true;

        final long grants = this.leaseGrants.sum();
        final long timeouts = this.leaseTimeouts.sum();
        final long queuedMillis = TimeUnit.NANOSECONDS.toMillis(this.leaseQueuedMaxNanos);
        final long parkedMillis = TimeUnit.NANOSECONDS.toMillis(this.leaseParkedMaxNanos);
        final String quiet = suppressed == 0L
            ? ""
            : " (" + suppressed + " more lease timeout pass(es) suppressed since the last warning)";
        LOGGER.warn(
            "Kairos: lease wait above the {} ms timeout: {} of {} grant(s) timed out, "
                + "longest queued {} ms, longest parked {} ms, fund {}/{}; the tick body is never blocked{}",
            TimeUnit.NANOSECONDS.toMillis(threshold),
            timeouts,
            grants,
            queuedMillis,
            parkedMillis,
            this.live.get(),
            this.maxThreads,
            quiet
        );
        KairosLeaseWaitEvent.emit(
            queuedMillis, parkedMillis, grants, timeouts,
            TimeUnit.NANOSECONDS.toMillis(threshold), this.live.get()
        );
    }

    /**
     * Reads the region id of a queued tick without linking the scheduler to Folia's classes.
     *
     * <p>The facade patch is deliberately limited to the deadline accessor, so the region is read from
     * the public field Folia already exposes. This only runs on a rate-limited warning and never on the
     * tick path; an unknown tick answers {@code -1} and the warning says so.
     *
     * @param tick queued tick
     * @return the region id, or {@code -1} when this tick is not a region handle
     */
    private static long regionId(final SchedulableTick tick) {
        try {
            final Object data = tick.getClass().getField("region").get(tick);
            return data == null ? -1L : data.getClass().getField("id").getLong(data);
        } catch (ReflectiveOperationException | RuntimeException _) {
            return -1L;
        }
    }

    /**
     * One supervisor decision, with the numbers behind it.
     *
     * @param kind   what the pass did
     * @param before fund size before the decision
     * @param after  intended fund size after it
     * @param target size the demand asked for
     */
    private record FundDecision(KairosSnapshot.Decision kind, int before, int after, int target) {

        private String describe() {
            if (this.kind == KairosSnapshot.Decision.IDLE) {
                return "target " + this.target;
            }
            return this.before + " -> " + this.after + ", target " + this.target;
        }
    }

    /**
     * Refills the spawn budget by elapsed time, so the pace does not depend on the pass interval.
     *
     * @param now current {@link System#nanoTime()}
     * @return {@code true} when at least one spawn is paid for
     */
    private boolean refillSpawnTokens(final long now) {
        if (this.spawnTokensUpdatedNanos == 0L) {
            this.spawnTokensUpdatedNanos = now;
            this.spawnTokens = 1.0d;
        } else {
            final long elapsed = now - this.spawnTokensUpdatedNanos;
            if (elapsed > 0L) {
                this.spawnTokens = Math.min(
                    this.tuning.spawnRatePerSecond(),
                    this.spawnTokens + (double) elapsed * this.tuning.spawnRatePerSecond() / 1_000_000_000.0d
                );
                this.spawnTokensUpdatedNanos = now;
            }
        }
        return this.spawnTokens >= 1.0d;
    }

    /**
     * Retires at most one parked runner that either exceeds the target or has outlived the keep
     * alive. A runner with a permit is never touched, and the most recently idle one goes first, so
     * the long lived threads (and their thread locals) stay.
     *
     * @param target fund size the current demand asks for
     * @param now    current {@link System#nanoTime()}
     * @return {@code true} when one runner was asked to leave
     */
    private boolean retireOne(final int target, final long now) {
        synchronized (this.scheduleLock) {
            if (now - this.nextRetireNanos < 0L) {
                return false;
            }
            final int liveNow = this.live.get();
            if (liveNow <= this.minThreads) {
                return false;
            }
            // a floor change shortens the wait: an operator asking for fewer threads should not sit
            // out the whole keep alive; the cooldown still paces the shrink to one per second
            final long requiredIdle = Math.min(
                this.tuning.keepAliveNanos(), Math.max(0L, now - this.floorChangedNanos)
            );
            Runner candidate = null;
            long candidateIdle = Long.MAX_VALUE;
            for (final Runner runner : this.slots) {
                if (runner == null || !runner.thread.isAlive()) {
                    continue;
                }
                if (runner.state.state != Runner.STATE_IDLE || !this.idleThreads.get(runner.slot)) {
                    continue;
                }
                final long idleFor = now - runner.idleSince;
                final boolean surplus = liveNow > target && idleFor >= RETIRE_COOLDOWN_NANOS;
                final boolean tooLongIdle = idleFor >= requiredIdle;
                if ((surplus || tooLongIdle) && idleFor < candidateIdle) {
                    candidate = runner;
                    candidateIdle = idleFor;
                }
            }
            if (candidate == null) {
                return false;
            }
            this.idleThreads.clear(candidate.slot);
            // the hint of a leaving runner dies with it: whatever it kept goes back to the one queue,
            // and the slot that may be reused by the next spawn has no stale owner left behind
            this.drainBacklog(candidate);
            candidate.retire();
            this.nextRetireNanos = now + RETIRE_COOLDOWN_NANOS;
            this.retireCount.incrementAndGet();
            this.pump();
            return true;
        }
    }

    /**
     * Moves every tick a runner kept for itself back to the global queue.
     *
     * <p>Called when the runner leaves the fund. The ticks were never leased, so they are scheduled
     * again by the normal order; a stale hint would otherwise point at a runner whose slot no longer
     * belongs to it.
     *
     * @param runner runner that is about to leave
     */
    private void drainBacklog(final Runner runner) {
        final PriorityQueue<ScheduledState> backlog = runner.localBacklog;
        if (backlog == null) {
            return;
        }
        ScheduledState kept;
        while ((kept = backlog.poll()) != null) {
            kept.localOwner = null;
            this.queued.add(kept);
            --this.localDepth;
        }
        if (this.localDepth < 0) {
            // defensive: the counter only feeds the demand signal, but a negative depth would poison it
            this.localDepth = 0;
        }
    }

    private void supervisorLoop() {
        long lastErrorLog = 0L;
        while (!this.halted) {
            Thread.interrupted();
            LockSupport.parkNanos(this.tuning.supervisorPeriodNanos());
            if (this.halted) {
                return;
            }
            try {
                this.superviseOnce();
            } catch (final Throwable failure) {
                final long now = System.nanoTime();
                if (now - lastErrorLog >= ERROR_LOG_INTERVAL_NANOS || lastErrorLog == 0L) {
                    lastErrorLog = now;
                    LOGGER.warn(
                        "Kairos: a supervisor pass failed; the fund keeps its current size of {} thread(s)",
                        this.live.get(),
                        failure
                    );
                }
            }
        }
    }

    private void logStart(final int floor) {
        final String tickKnobs = this.describeKernel();
        if (!this.elastic) {
            LOGGER.info(
                "Kairos: fixed fund of {} tick thread(s) (elasticity is off); threaded-regions.threads "
                    + "needs a restart; {}",
                floor,
                tickKnobs
            );
            return;
        }
        if (this.maxThreads <= floor) {
            LOGGER.warn(
                "Kairos: fund is fixed at {} tick thread(s): the ceiling {} gives no headroom ({}); {}",
                floor,
                this.maxThreads,
                this.budgetSource,
                tickKnobs
            );
            return;
        }
        LOGGER.info(
            "Kairos: elastic fund min {}, max {} ({}), keep-alive {}s, spawn rate {}/s; {}",
            floor,
            this.maxThreads,
            this.budgetSource,
            this.tuning.keepAliveMillis() / 1000L,
            this.tuning.spawnRatePerSecond(),
            tickKnobs
        );
    }

    /**
     * Describes the tick level knobs in one clause for the startup line.
     *
     * @return a short human readable summary, never {@code null}
     */
    private String describeKernel() {
        final KairosKernel knobs = this.kernel;
        return "catch-up " + knobs.catchUpCapTicks() + " tick(s)/"
            + (knobs.catchUpBudgetMillis() > 0 ? knobs.catchUpBudgetMillis() + " ms" : "off")
            + ", affinity queue " + knobs.affinityQueueSize()
            + " (steal " + knobs.stealThresholdMillis() + " ms), lease timeout "
            + (knobs.leaseTimeoutMillis() > 0 ? knobs.leaseTimeoutMillis() + " ms" : "off");
    }

    /**
     * Resolves the ceiling of the fund without a second copy of the processor arithmetic.
     *
     * <p>An explicit {@code max-threads} always wins. Otherwise the thread layer's budget port answers,
     * because its controller is the one place that knows what is left after the deducted lanes and the
     * reserve; its number is used as is, never reduced again. Without a running thread layer the
     * documented core fraction is the fallback. A ceiling below the floor is not a startup failure: the
     * fund stays at the floor and the snapshot says {@code NO_HEADROOM}.
     *
     * @param floor      requested floor, which the ceiling is never silently below
     * @param configured operator ceiling, or {@code -1} for the runtime budget
     * @param budget     budget port of a running thread layer, or {@code null}
     * @return the ceiling to use
     */
    private int resolveMax(
        final int floor,
        final int configured,
        final @Nullable KairosBudgetPort budget
    ) {
        if (configured > 0) {
            this.budgetSource = "kairos.max-threads " + configured;
            if (configured < floor) {
                LOGGER.warn(
                    "Kairos: kairos.max-threads {} is below the {} requested tick threads; the fund stays fixed",
                    configured,
                    floor
                );
                return floor;
            }
            return configured;
        }
        if (budget != null) {
            final int ceiling;
            try {
                ceiling = budget.ceiling(floor);
            } catch (final RuntimeException failure) {
                LOGGER.warn("Kairos: the budget port failed; falling back to the core count", failure);
                return this.coreCeiling(floor);
            }
            this.budgetSource = budget.describe();
            if (ceiling < floor) {
                this.budgetSource = budget.describe() + " (budget " + ceiling + " below floor " + floor + ')';
                LOGGER.warn(
                    "Kairos: the budget allows {} tick thread(s), below the floor {}; the fund stays fixed",
                    ceiling,
                    floor
                );
                return floor;
            }
            return ceiling;
        }
        return this.coreCeiling(floor);
    }

    /**
     * Resolves the fallback ceiling from the core count, the same 80% rule the compute budget uses.
     *
     * @param floor requested floor
     * @return the fallback ceiling, never below the floor
     */
    private int coreCeiling(final int floor) {
        final int cores = Runtime.getRuntime().availableProcessors();
        final int usable = (int) Math.floor(cores * USABLE_CORE_FRACTION);
        this.budgetSource = "floor(" + cores + " cores * 0.8)";
        return Math.max(floor, usable);
    }

    private static final class ScheduledState {

        private final SchedulableTick tick;
        private KairosScheduler schedulerOwnedBy;
        private @Nullable Runner ownedBy;
        private LinkedSortedSet.@Nullable Link<ScheduledState> awaitingLink;

        /** Runner whose private queue holds this tick, or {@code null} when it sits in the one queue. */
        private @Nullable Runner localOwner;

        /** When the permit was handed out, or zero before the first grant. */
        private long grantedNanos;

        /** Start of the previous body, for the observed period sample; zero before the first body. */
        private long lastStartNanos;

        /** Catch-up bodies already run in the current burst; runner thread only. */
        private int burstBodies;

        /** When the current catch-up burst started; runner thread only. */
        private long burstStartNanos;

        /** Periods the handle reported before the body, or {@code -1}; runner thread only. */
        private long bodyMissedPeriods = -1L;

        /** Whether the body about to run accounts for a single period; runner thread only. */
        private boolean catchUpBody;

        private ScheduledState(final SchedulableTick tick) {
            this.tick = tick;
        }
    }

    /**
     * One thread of the fund. Not a {@link Thread} subclass: the thread object comes from the
     * caller's factory so the tick context stays Folia's.
     */
    private static final class Runner implements Runnable {

        /** Parked with no permit and no waiting tick; stateTarget is {@code null}. */
        private static final int STATE_IDLE = 0;

        /** Holds the permit for one tick and waits for its deadline; stateTarget is the tick. */
        private static final int STATE_AWAITING_TICK = 1;

        /** Inside {@code runTick()}; stateTarget is the tick being executed. */
        private static final int STATE_EXECUTING_TICK = 2;

        /** Asked to leave the fund. Only ever entered from {@link #STATE_IDLE}. */
        private static final int STATE_RETIRING = 3;

        private static final VarHandle STATE_HANDLE = ConcurrentUtil.getVarHandle(
            Runner.class, "state", TickThreadRunnerState.class
        );

        private final Thread thread;
        private final int slot;
        private final KairosScheduler scheduler;

        private volatile TickThreadRunnerState state = new TickThreadRunnerState(null, STATE_IDLE);

        /**
         * When this runner last became idle, read by the supervisor under the schedule lock.
         */
        private volatile long idleSince;

        /**
         * Ticks kept for this runner while soft affinity is on, or {@code null} when it is off.
         * Guarded by {@link KairosScheduler#scheduleLock}.
         */
        private @Nullable PriorityQueue<ScheduledState> localBacklog;

        private Runner(final Thread thread, final int slot, final KairosScheduler scheduler) {
            this.thread = thread;
            this.slot = slot;
            this.scheduler = scheduler;
            this.idleSince = scheduler.clock.getAsLong();
        }

        /**
         * Allocates the private queue when the operator asked for soft affinity. Called before the
         * thread starts, never on the tick path.
         */
        private void prepareBacklog() {
            if (this.scheduler.affinityQueueSize > 0 && this.localBacklog == null) {
                this.localBacklog = new PriorityQueue<>(this.scheduler.byDeadline);
            }
        }

        private void setStatePlain(final TickThreadRunnerState state) {
            STATE_HANDLE.set(this, state);
        }

        private void setStateOpaque(final TickThreadRunnerState state) {
            STATE_HANDLE.setOpaque(this, state);
        }

        private void setStateVolatile(final TickThreadRunnerState state) {
            STATE_HANDLE.setVolatile(this, state);
        }

        private record TickThreadRunnerState(@Nullable ScheduledState stateTarget, int state) {
        }

        private Thread getRunnerThread() {
            return this.thread;
        }

        private void forceIdle() {
            final TickThreadRunnerState state = this.state;
            if (state.state != STATE_AWAITING_TICK) {
                throw new IllegalStateException("Cannot force idle in state " + state);
            }
            state.stateTarget.ownedBy = null;
            this.idleSince = this.scheduler.clock.getAsLong();
            this.setStateOpaque(new TickThreadRunnerState(null, STATE_IDLE));
            // no need to unpark
        }

        /**
         * Asks the runner to leave the fund. Called by the supervisor with the schedule lock held,
         * after the permit bit was cleared, and only from {@link #STATE_IDLE}.
         */
        private void retire() {
            this.setStateVolatile(new TickThreadRunnerState(null, STATE_RETIRING));
            LockSupport.unpark(this.getRunnerThread());
        }

        private boolean takeTask(final TickThreadRunnerState state, final ScheduledState task) {
            synchronized (this.scheduler.scheduleLock) {
                if (this.state != state) {
                    return false;
                }
                this.setStatePlain(new TickThreadRunnerState(task, STATE_EXECUTING_TICK));
                this.scheduler.takeTask(this, task);
                return true;
            }
        }

        private void returnTask(final ScheduledState task, final boolean reschedule) {
            synchronized (this.scheduler.scheduleLock) {
                task.ownedBy = null;

                final ScheduledState next = this.scheduler.returnTask(this, reschedule ? task : null);
                if (next == null) {
                    this.idleSince = this.scheduler.clock.getAsLong();
                    this.setStatePlain(new TickThreadRunnerState(null, STATE_IDLE));
                }
                // when there is a next tick, the scheduler already linked it and moved this runner to
                // AWAITING_TICK in the same transition; nothing is left to set here
                if (!reschedule) {
                    this.scheduler.release(task);
                }
            }
        }

        @Override
        public void run() {
            try {
                this.loop();
            } finally {
                this.scheduler.live.decrementAndGet();
            }
        }

        private void loop() {
            while (true) {
                final TickThreadRunnerState startState = this.state;
                final int startStateType = startState.state;
                final ScheduledState startStateTask = startState.stateTarget;

                if (this.scheduler.halted) {
                    return;
                }

                switch (startStateType) {
                    case STATE_IDLE -> {
                        while (this.state.state == STATE_IDLE) {
                            Thread.interrupted();
                            // bounded on purpose: halt sets its flag before it unparks, and a park that
                            // lands between the two would otherwise never be woken
                            LockSupport.parkNanos(IDLE_PARK_NANOS);
                            if (this.scheduler.halted) {
                                return;
                            }
                        }
                    }

                    case STATE_AWAITING_TICK -> {
                        final long deadline = this.scheduler.scheduledStart.applyAsLong(startStateTask.tick);
                        while (this.state == startState) {
                            Thread.interrupted();
                            final long diff = deadline - System.nanoTime();
                            if (diff <= 0L) {
                                break;
                            }
                            LockSupport.parkNanos(startState, diff);
                            if (this.scheduler.halted) {
                                return;
                            }
                        }

                        if (this.state != startState || !this.takeTask(startState, startStateTask)) {
                            continue;
                        }

                        // the body may be a catch-up body; the decision is made before the handle moves
                        this.scheduler.prepareBody(startStateTask);
                        final long bodyStart = System.nanoTime();

                        // failures reach Folia's uncaught handler, which halts and stops the server
                        final boolean reschedule = startStateTask.tick.runTick();
                        final long bodyEnd = System.nanoTime();

                        this.scheduler.recordTick(startStateTask, bodyStart, bodyEnd);
                        this.returnTask(startStateTask, reschedule);
                    }

                    case STATE_RETIRING -> {
                        return;
                    }

                    case STATE_EXECUTING_TICK -> throw new IllegalStateException(
                        "Tick execution must be set by runner thread, not by any other thread"
                    );

                    default -> throw new IllegalStateException("Unknown state: " + startState);
                }
            }
        }
    }
}
