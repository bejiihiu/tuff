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
import java.util.concurrent.locks.LockSupport;
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

    private final ToLongFunction<SchedulableTick> scheduledStart;
    private final ThreadFactory threadFactory;

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

    private volatile int minThreads;

    /** Equal to {@code minThreads} until the tuning is known; then the resolved ceiling. */
    private volatile int maxThreads;

    private volatile boolean elastic;
    private volatile KairosTuning tuning = KairosTuning.fixed();
    private volatile boolean started;
    private volatile boolean halted;

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
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be positive, got " + threads);
        }
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        this.scheduledStart = Objects.requireNonNull(scheduledStart, "scheduledStart");

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
        synchronized (this.lifecycleLock) {
            if (this.halted) {
                throw new IllegalStateException("Kairos was halted before it started");
            }
            if (this.started) {
                throw new IllegalStateException("Kairos was already started");
            }
            this.tuning = fundTuning;
            this.elastic = fundTuning.elastic();
            final int floor = this.minThreads;
            this.maxThreads = this.elastic ? resolveMax(floor, fundTuning.maxThreads()) : floor;
            this.started = true;
            this.floorChangedNanos = System.nanoTime();

            for (final Runner runner : this.slots) {
                runner.thread.start();
                this.live.incrementAndGet();
            }

            if (this.elastic && this.maxThreads > floor) {
                final Thread created = new Thread(this::supervisorLoop, "Tuff Kairos Supervisor");
                created.setDaemon(true);
                created.setUncaughtExceptionHandler((final Thread thread, final Throwable failure) ->
                    LOGGER.error("The Kairos supervisor died; the fund keeps its current size", failure)
                );
                this.supervisor = created;
                created.start();
            }
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
            this.floorChangedNanos = System.nanoTime();
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
            if (state.awaitingLink != null) {
                this.awaiting.remove(state.awaitingLink);
                state.awaitingLink = null;

                final Runner runner = state.ownedBy;
                final ScheduledState replace = this.queued.poll();
                if (replace == null) {
                    this.idleThreads.set(runner.slot);
                    runner.forceIdle();
                } else {
                    runner.replaceTask(replace);
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
            this.idleThreads.clear(firstIdleThread);
            final Runner runner = runners[firstIdleThread];
            task.awaitingLink = this.awaiting.addLast(task);
            runner.acceptTask(task);
            return false;
        }

        // try to replace the last awaiting task, the same eviction EDF performs
        final ScheduledState last = this.awaiting.last();
        if (last != null && this.compareByDeadline(task, last) < 0) {
            this.awaiting.pollLast();
            last.awaitingLink = null;
            task.awaitingLink = this.awaiting.addLast(task);
            this.queued.add(last);

            final Runner runner = last.ownedBy;
            runner.replaceTask(task);
            this.recordDemand();
            return false;
        }

        this.queued.add(task);
        this.recordDemand();
        return true;
    }

    /**
     * Remembers that a tick had to wait, so the supervisor sees the burst even when it samples a quiet
     * moment. Guarded by {@link #scheduleLock}.
     */
    private void recordDemand() {
        this.peakDemand = Math.max(this.peakDemand, this.live.get() + this.queued.size());
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
    }

    private @Nullable ScheduledState returnTask(final Runner runner, final @Nullable ScheduledState reschedule) {
        if (reschedule != null) {
            this.queued.add(reschedule);
        }
        final ScheduledState ret = this.queued.poll();
        if (ret == null) {
            this.idleThreads.set(runner.slot);
            return null;
        }
        ret.awaitingLink = this.awaiting.addLast(ret);
        return ret;
    }

    private void release(final ScheduledState task) {
        // the tick will not reschedule, so this scheduler must not keep the handle alive
        this.states.remove(task.tick);
    }

    /**
     * Hands queued ticks to permits that just became free. Called after a spawn, with the schedule
     * lock held.
     */
    private void pump() {
        int slot;
        while ((slot = this.idleThreads.nextSetBit(0)) >= 0) {
            final ScheduledState next = this.queued.poll();
            if (next == null) {
                return;
            }
            this.idleThreads.clear(slot);
            final Runner runner = this.slots[slot];
            next.awaitingLink = this.awaiting.addLast(next);
            runner.acceptTask(next);
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
     */
    private void superviseOnce() {
        final long now = System.nanoTime();
        final int target;
        synchronized (this.scheduleLock) {
            int busy = 0;
            for (final Runner runner : this.slots) {
                if (runner == null || !runner.thread.isAlive()) {
                    continue;
                }
                final int state = runner.state.state;
                if (state == Runner.STATE_AWAITING_TICK || state == Runner.STATE_EXECUTING_TICK) {
                    ++busy;
                }
            }
            // a tick that had to wait means every permit was taken at that moment, so the peak is a
            // better demand signal than a single sample of a queue that drains in microseconds
            final int peak = this.peakDemand;
            this.peakDemand = 0;
            final int demand = Math.max(busy + this.queued.size(), peak);
            target = Math.max(this.minThreads, Math.min(this.maxThreads, demand));
        }

        final int before = this.live.get();
        if (before < target && this.refillSpawnTokens(now) && this.spawn()) {
            this.spawnTokens -= 1.0d;
            // the runner is counted in live immediately, so the intended size is exact
            final int after = before + 1;
            if (after >= this.maxThreads) {
                LOGGER.info("Kairos: fund reached its ceiling of {} tick thread(s) under load", after);
            } else {
                LOGGER.debug("Kairos: fund {} -> {} tick thread(s), target {}", before, after, target);
            }
            // one fund decision per pass: a spawn never races a retire in the same second
            return;
        }
        final int liveNow = this.live.get();
        if (liveNow > this.minThreads && this.retireOne(target, now)) {
            // the parked runner may need a moment to leave its loop, so the intended size is logged
            final int after = liveNow - 1;
            if (after <= this.minThreads) {
                LOGGER.info("Kairos: fund is back to its floor of {} tick thread(s)", after);
            } else {
                LOGGER.debug("Kairos: fund {} -> {} tick thread(s), target {}", liveNow, after, target);
            }
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
            candidate.retire();
            this.nextRetireNanos = now + RETIRE_COOLDOWN_NANOS;
            this.retireCount.incrementAndGet();
            return true;
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
        if (!this.elastic) {
            LOGGER.info(
                "Kairos: fixed fund of {} tick thread(s) (elasticity is off); threaded-regions.threads "
                    + "needs a restart",
                floor
            );
            return;
        }
        if (this.maxThreads <= floor) {
            LOGGER.warn(
                "Kairos: fund is fixed at {} tick thread(s): the ceiling {} gives no headroom",
                floor,
                this.maxThreads
            );
            return;
        }
        final int cores = Runtime.getRuntime().availableProcessors();
        final String ceilingSource = this.tuning.maxThreads() > 0
            ? "kairos.max-threads " + this.tuning.maxThreads()
            : "floor(" + cores + " cores * 0.8)";
        LOGGER.info(
            "Kairos: elastic fund min {}, max {} ({}), keep-alive {}s, spawn rate {}/s",
            floor,
            this.maxThreads,
            ceilingSource,
            this.tuning.keepAliveMillis() / 1000L,
            this.tuning.spawnRatePerSecond()
        );
    }

    /**
     * Resolves the default ceiling from the core count, using the same 80% rule as the compute budget.
     * A budget aware ceiling (chunk, IO and compute threads subtracted) is a later package; until then
     * the core count is the honest bound, and the operator can pick a number.
     *
     * @param floor      requested floor, which the ceiling is never below
     * @param configured operator ceiling, or {@code -1} for the core count rule
     * @return the ceiling to use
     */
    private static int resolveMax(final int floor, final int configured) {
        if (configured > 0) {
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
        final int usable = (int) Math.floor(Runtime.getRuntime().availableProcessors() * USABLE_CORE_FRACTION);
        return Math.max(floor, usable);
    }

    private static final class ScheduledState {

        private final SchedulableTick tick;
        private KairosScheduler schedulerOwnedBy;
        private @Nullable Runner ownedBy;
        private LinkedSortedSet.@Nullable Link<ScheduledState> awaitingLink;

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
        private volatile long idleSince = System.nanoTime();

        private Runner(final Thread thread, final int slot, final KairosScheduler scheduler) {
            this.thread = thread;
            this.slot = slot;
            this.scheduler = scheduler;
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

        private void acceptTask(final ScheduledState task) {
            if (task.ownedBy != null) {
                throw new IllegalStateException("Already owned by another runner");
            }
            task.ownedBy = this;
            final TickThreadRunnerState state = this.state;
            if (state.state != STATE_IDLE) {
                throw new IllegalStateException("Cannot accept task in state " + state);
            }
            this.setStateVolatile(new TickThreadRunnerState(task, STATE_AWAITING_TICK));
            LockSupport.unpark(this.getRunnerThread());
        }

        private void replaceTask(final ScheduledState task) {
            final TickThreadRunnerState state = this.state;
            if (state.state != STATE_AWAITING_TICK) {
                throw new IllegalStateException("Cannot replace task in state " + state);
            }
            if (task.ownedBy != null) {
                throw new IllegalStateException("Already owned by another runner");
            }
            task.ownedBy = this;

            state.stateTarget.ownedBy = null;

            this.setStateVolatile(new TickThreadRunnerState(task, STATE_AWAITING_TICK));
            LockSupport.unpark(this.getRunnerThread());
        }

        private void forceIdle() {
            final TickThreadRunnerState state = this.state;
            if (state.state != STATE_AWAITING_TICK) {
                throw new IllegalStateException("Cannot force idle in state " + state);
            }
            state.stateTarget.ownedBy = null;
            this.idleSince = System.nanoTime();
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

                final ScheduledState newWait = this.scheduler.returnTask(this, reschedule ? task : null);
                if (newWait == null) {
                    this.idleSince = System.nanoTime();
                    this.setStatePlain(new TickThreadRunnerState(null, STATE_IDLE));
                } else {
                    if (newWait.ownedBy != null) {
                        throw new IllegalStateException("Already owned by another runner");
                    }
                    newWait.ownedBy = this;
                    this.setStatePlain(new TickThreadRunnerState(newWait, STATE_AWAITING_TICK));
                }
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
                            LockSupport.park();
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

                        // failures reach Folia's uncaught handler, which halts and stops the server
                        final boolean reschedule = startStateTask.tick.runTick();

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
