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
import java.util.BitSet;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToLongFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Deadline-ordered tick scheduler with a fixed fund of threads.
 *
 * <p>This is the first Kairos prototype: the queue, the comparator and the runner state machine are
 * ported one to one from {@code ca.spottedleaf.concurrentutil.scheduler.EDFSchedulerThreadPool}
 * (leafpile 1.3.3), so tick order, tie breaks and the "a late region does not starve the rest"
 * property are exactly Folia's. The only structural difference is the fund: every thread is created
 * in the constructor and started together, so {@code min == max} and nothing is resized. Elasticity,
 * budgets, affinity and catch up are later packages and are not implemented here.
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

    private final ToLongFunction<SchedulableTick> scheduledStart;

    private final Fund fund;
    private final LinkedSortedSet<ScheduledState> awaiting;
    private final PriorityQueue<ScheduledState> queued;
    private final BitSet idleThreads;

    /**
     * Ownership of a tick while it is scheduled here. The map replaces the state field leafpile keeps
     * inside {@link SchedulableTick}; entries are dropped as soon as the tick stops rescheduling.
     */
    private final Map<SchedulableTick, ScheduledState> states = new IdentityHashMap<>();

    private final Object scheduleLock = new Object();
    private final Object lifecycleLock = new Object();

    private volatile boolean halted;

    /**
     * Creates every thread of the fund. Nothing runs until {@link #start()}.
     *
     * @param threads        fixed number of tick threads; the fund never grows or shrinks
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

        final BitSet idleThreads = new BitSet(threads);
        for (int i = 0; i < threads; ++i) {
            idleThreads.set(i);
        }
        this.idleThreads = idleThreads;

        this.fund = new Fund(threads, threadFactory, this);
        LOGGER.info(
            "Kairos: fixed fund of {} tick thread(s) (min == max); threaded-regions.threads is restart-only",
            threads
        );
    }

    /**
     * Starts every thread of the fund, after checking the experimental opt-in.
     *
     * <p>The gate lives here rather than in the constructor because the constructor runs while
     * {@code paper-global.yml} is being loaded, before {@code config/tuff.yml} is read.
     *
     * @throws IllegalStateException if the operator did not opt in, the scheduler was halted, or it
     *                               was already started
     */
    public void start() {
        KairosGate.requireOptIn();
        synchronized (this.lifecycleLock) {
            if (this.halted) {
                throw new IllegalStateException("Kairos was halted before it started");
            }
            if (this.fund.started()) {
                throw new IllegalStateException("Kairos was already started");
            }
            this.fund.markStarted();
            for (final Thread thread : this.fund.threads()) {
                thread.start();
            }
        }
    }

    /**
     * Records the configured thread count.
     *
     * <p>The fund is fixed for this prototype, so the only honest answer to a different value is a
     * warning that it takes a restart. This is also where the "threads is a silent no op" problem of
     * EDF is fixed: the operator gets a line instead of nothing.
     *
     * @param threads requested tick thread count
     */
    public void setThreads(final int threads) {
        if (threads == this.fund.size()) {
            LOGGER.debug("Kairos: threaded-regions.threads is {} and already matches the fixed fund", threads);
            return;
        }
        LOGGER.warn(
            "Kairos: threaded-regions.threads is {} but the fund is fixed at {} (min == max); "
                + "changing it needs a restart",
            threads,
            this.fund.size()
        );
    }

    /**
     * Returns the live threads of the fund, including any that are still finishing a tick.
     *
     * @return a fresh array, never {@code null}
     */
    @Override
    public Thread[] getAliveThreads() {
        final List<Thread> ret = new ArrayList<>(this.fund.size());
        for (final Thread thread : this.fund.everCreated()) {
            if (thread.isAlive()) {
                ret.add(thread);
            }
        }
        return ret.toArray(new Thread[0]);
    }

    /**
     * Returns the threads allocated for executing ticks. There is no retiring state in a fixed fund,
     * so this is the same set as {@link #getAliveThreads()}.
     *
     * @return a fresh array, never {@code null}
     */
    @Override
    public Thread[] getCoreThreads() {
        return this.getAliveThreads();
    }

    /**
     * Stops every runner at its next loop check without interrupting a running tick.
     *
     * <p>Idempotent, never blocks and is safe from a tick thread, which is what the shutdown path
     * needs.
     */
    @Override
    public void halt() {
        this.halted = true;
        for (final Thread thread : this.fund.everCreated()) {
            // force a response even when the runner is parked
            LockSupport.unpark(thread);
        }
    }

    /**
     * Waits for every thread ever created, swallowing interrupts like the EDF original.
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
     * Waits for every thread ever created and propagates an interrupt.
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
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(msToWait);
        boolean interrupted = false;
        try {
            for (final Thread thread : this.fund.everCreated()) {
                while (thread.isAlive()) {
                    try {
                        if (msToWait > 0L) {
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
        synchronized (this.scheduleLock) {
            if (this.states.containsKey(tick)) {
                throw new IllegalStateException("Task " + tick + " is already scheduled");
            }
            final ScheduledState state = new ScheduledState(tick);
            this.states.put(tick, state);
            state.schedulerOwnedBy = this;
            this.insertFresh(state);
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

    private void insertFresh(final ScheduledState task) {
        final Runner[] runners = this.fund.slots();

        final int firstIdleThread = this.idleThreads.nextSetBit(0);
        if (firstIdleThread != -1) {
            this.idleThreads.clear(firstIdleThread);
            final Runner runner = runners[firstIdleThread];
            task.awaitingLink = this.awaiting.addLast(task);
            runner.acceptTask(task);
            return;
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
            return;
        }

        this.queued.add(task);
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

    private @Nullable ScheduledState returnTask(final @Nullable ScheduledState reschedule) {
        if (reschedule != null) {
            this.queued.add(reschedule);
        }
        final ScheduledState ret = this.queued.poll();
        if (ret == null) {
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
     * A fixed set of runners and their threads.
     *
     * <p>{@code everCreated} exists for the join contract, not for elasticity: every thread is made
     * here and none is ever retired, so it is the same set for the lifetime of the scheduler.
     */
    private static final class Fund {

        private final Runner[] slots;
        private final Thread[] threads;
        private final List<Thread> everCreated;
        private volatile boolean started;

        private Fund(final int size, final ThreadFactory threadFactory, final KairosScheduler scheduler) {
            final Runner[] runners = new Runner[size];
            final Thread[] created = new Thread[size];
            final List<Thread> all = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                final LazyRunnable run = new LazyRunnable();
                final Thread thread = created[i] = threadFactory.newThread(run);
                all.add(thread);
                run.setRunnable(runners[i] = new Runner(thread, i, scheduler));
            }
            this.slots = runners;
            this.threads = created;
            this.everCreated = List.copyOf(all);
        }

        private int size() {
            return this.slots.length;
        }

        private Runner[] slots() {
            return this.slots;
        }

        private Thread[] threads() {
            return this.threads;
        }

        private List<Thread> everCreated() {
            return this.everCreated;
        }

        private boolean started() {
            return this.started;
        }

        private void markStarted() {
            this.started = true;
        }
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

        private static final VarHandle STATE_HANDLE = ConcurrentUtil.getVarHandle(
            Runner.class, "state", TickThreadRunnerState.class
        );

        private final Thread thread;
        private final int slot;
        private final KairosScheduler scheduler;

        private volatile TickThreadRunnerState state = new TickThreadRunnerState(null, STATE_IDLE);

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
            this.setStateOpaque(new TickThreadRunnerState(null, STATE_IDLE));
            // no need to unpark
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

                final ScheduledState newWait = this.scheduler.returnTask(reschedule ? task : null);
                if (newWait == null) {
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

                    case STATE_EXECUTING_TICK -> throw new IllegalStateException(
                        "Tick execution must be set by runner thread, not by any other thread"
                    );

                    default -> throw new IllegalStateException("Unknown state: " + startState);
                }
            }
        }
    }
}
