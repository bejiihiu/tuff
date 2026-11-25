package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;
import kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop;
import kz.bejiihiu.tuff.compute.internal.worker.WorkerPool;
import org.jspecify.annotations.Nullable;

/**
 * Reusable pool of platform worker threads.
 *
 * <p>Each worker owns one platform thread and repeatedly calls
 * {@link WorkerLoop#executeNext(int)}. The pool never creates a thread per task: a worker stays
 * alive across tasks and is only retired by keep-alive, by an explicit shrink or by shutdown. This
 * is also why virtual threads are rejected. They add no parallelism for CPU bound work (JEP 444),
 * they hide the worker from a normal thread dump, and they cannot be pinned to a stable worker index
 * that the dispatcher uses for its own batch and work stealing.
 *
 * <p>Lifecycle:
 * <ul>
 *     <li>{@link #start()} creates the configured initial number of workers exactly once;</li>
 *     <li>{@link #shutdown()} and {@link #shutdownNow()} stop new claims; a task that is already
 *     inside {@code executeNext} is allowed to finish because the pool never interrupts a worker;</li>
 *     <li>{@link #awaitTermination(Duration)} returns after every worker thread has left its run
 *     method, which is also the only moment {@link #isTerminated()} becomes {@code true}.</li>
 * </ul>
 *
 * <p>Idle handshake with the loop: {@code executeNext} must return {@code false} when it found no
 * runnable task and had to wait. The pool then marks the worker idle, applies keep-alive and parks
 * for a bounded time before calling the loop again. Returning {@code true} means the worker either
 * executed a task or deliberately wants to continue and resets the idle timer. A loop that always
 * returns {@code true} cannot be kept alive/retired by the pool; it is responsible for its own
 * bounded wait, as the contract allows.
 *
 * <p>Threads are created through the supplied {@link java.util.concurrent.ThreadFactory}. Production
 * wiring should use {@link kz.bejiihiu.tuff.compute.internal.util.ThreadFactories#platform(String,
 * boolean)} with {@code daemon = true}: a worker that is stuck in user code must never keep the JVM
 * alive after the server tried to stop, and the server already has an explicit shutdown sequence.
 */
public final class DynamicWorkerPool implements WorkerPool {

    private static final System.Logger LOG = System.getLogger(DynamicWorkerPool.class.getName());
    private static final int MAX_LOOP_FAILURES = 3;
    private static final int RETIRE_NONE = 0;
    private static final int RETIRE_SURPLUS = 1;
    private static final int RETIRE_IDLE = 2;

    private final WorkerLoop loop;
    private final ComputeClock clock;
    private final ThreadFactory threadFactory;

    private final int minWorkers;
    private final int maxWorkers;
    private final int initialWorkers;
    private final boolean automatic;

    private final long keepAliveNanos;
    private final long idleWaitNanos;

    private final Object lifecycleLock = new Object();
    private final Worker[] slots;
    private final Set<Worker> workers = ConcurrentHashMap.newKeySet();
    private final Set<Thread> createdThreads = ConcurrentHashMap.newKeySet();
    private final AtomicInteger liveWorkers = new AtomicInteger();
    private final AtomicInteger exitReservations = new AtomicInteger();
    private final AtomicInteger desiredWorkers = new AtomicInteger();
    private final AtomicInteger retirementFloor = new AtomicInteger();
    private volatile int peakWorkers;

    private final ConcurrencyGate gate;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    private final ReentrantLock terminationLock = new ReentrantLock();
    private final Condition terminated = this.terminationLock.newCondition();

    private volatile boolean explicitLimit;

    /**
     * Creates a pool from the full scheduler configuration.
     *
     * @param settings full scheduler settings, worker and scheduling sections are used
     * @param loop     loop the workers execute, implemented by the engine
     * @param clock    clock used for keep-alive measurements
     * @param factory  factory for platform worker threads
     */
    public DynamicWorkerPool(
        final ComputeSettings settings,
        final WorkerLoop loop,
        final ComputeClock clock,
        final ThreadFactory factory
    ) {
        this(
            settings.workers(),
            settings.scheduling(),
            Runtime.getRuntime().availableProcessors(),
            loop,
            clock,
            factory);
    }

    /**
     * Creates a pool sized for the current JVM.
     *
     * @param workers    worker sizing from the configuration
     * @param scheduling scheduling options, used for the idle wait
     * @param loop       loop the workers execute, implemented by the engine
     * @param clock      clock used for keep-alive measurements
     * @param factory    factory for platform worker threads
     */
    public DynamicWorkerPool(
        final ComputeSettings.Workers workers,
        final ComputeSettings.Scheduling scheduling,
        final WorkerLoop loop,
        final ComputeClock clock,
        final ThreadFactory factory
    ) {
        this(workers, scheduling, Runtime.getRuntime().availableProcessors(), loop, clock, factory);
    }

    /**
     * Creates a pool with an explicit processor count.
     *
     * <p>Tests use this overload to make automatic sizing deterministic. Production code should use
     * the constructor above so {@code -XX:ActiveProcessorCount} is honoured.
     *
     * @param workers             worker sizing from the configuration
     * @param scheduling          scheduling options, used for the idle wait
     * @param availableProcessors processors available to the JVM, at least {@code 1}
     * @param loop                loop the workers execute, implemented by the engine
     * @param clock               clock used for keep-alive measurements
     * @param factory             factory for platform worker threads
     */
    public DynamicWorkerPool(
        final ComputeSettings.Workers workers,
        final ComputeSettings.Scheduling scheduling,
        final int availableProcessors,
        final WorkerLoop loop,
        final ComputeClock clock,
        final ThreadFactory factory
    ) {
        Preconditions.requireNonNull(scheduling, "scheduling");
        this.loop = Preconditions.requireNonNull(loop, "loop");
        this.clock = Preconditions.requireNonNull(clock, "clock");
        this.threadFactory = Preconditions.requireNonNull(factory, "factory");
        Preconditions.checkAtLeast(availableProcessors, 1, "availableProcessors");

        this.maxWorkers = WorkerSizing.maxWorkers(workers, availableProcessors);
        this.minWorkers = Math.min(workers.minThreads(), this.maxWorkers);
        this.initialWorkers = WorkerSizing.targetWorkers(workers, availableProcessors);
        this.automatic = workers.automaticThreadCount();
        this.keepAliveNanos = workers.keepAlive().toNanos();
        this.idleWaitNanos = scheduling.idleWait().toNanos();

        this.slots = new Worker[this.maxWorkers];
        this.desiredWorkers.set(this.initialWorkers);
        this.retirementFloor.set(this.automatic ? this.minWorkers : this.initialWorkers);
        this.gate = new ConcurrencyGate(this.initialWorkers);
    }

    @Override
    public void start() {
        if (!this.state.compareAndSet(State.NEW, State.RUNNING)) {
            return;
        }
        if (this.isShutdown()) {
            return;
        }
        if (!this.explicitLimit) {
            this.gate.setLimit(this.desiredWorkers.get());
        }
        try {
            this.spawnUpTo(this.desiredWorkers.get());
        } catch (RuntimeException | Error failure) {
            this.requestShutdown();
            this.wakeAll();
            this.checkTerminated();
            throw failure;
        }
        this.checkTerminated();
    }

    @Override
    public void shutdown() {
        this.requestShutdown();
        this.wakeAll();
        this.checkTerminated();
    }

    @Override
    public void shutdownNow() {
        this.shutdown();
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        Preconditions.checkNonNegative(timeout.toNanos(), "timeout");
        final long deadline = System.nanoTime() + timeout.toNanos();
        boolean terminatedInTime = false;
        this.terminationLock.lockInterruptibly();
        try {
            while (this.state.get() != State.TERMINATED) {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    break;
                }
                this.terminated.awaitNanos(remaining);
            }
            terminatedInTime = this.state.get() == State.TERMINATED;
        } finally {
            this.terminationLock.unlock();
        }
        if (!terminatedInTime) {
            return false;
        }
        return this.joinWorkers(deadline);
    }

    private boolean joinWorkers(final long deadline) throws InterruptedException {
        for (final Thread thread : this.createdThreads) {
            if (thread == Thread.currentThread()) {
                continue;
            }
            final long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                if (thread.isAlive()) {
                    return false;
                }
                continue;
            }
            thread.join(Math.max(1L, remaining / 1_000_000L), (int) (remaining % 1_000_000L));
            if (thread.isAlive()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isShutdown() {
        final State current = this.state.get();
        return current == State.SHUTDOWN || current == State.TERMINATED;
    }

    @Override
    public boolean isTerminated() {
        return this.state.get() == State.TERMINATED;
    }

    @Override
    public int workerCount() {
        return this.liveWorkers.get();
    }

    @Override
    public int activeWorkers() {
        return this.gate.inUse();
    }

    @Override
    public int idleWorkers() {
        return Math.max(0, this.liveWorkers.get() - this.gate.inUse());
    }

    @Override
    public int peakWorkers() {
        return this.peakWorkers;
    }

    @Override
    public int concurrencyLimit() {
        return this.gate.limit();
    }

    @Override
    public void setConcurrencyLimit(final int permits) {
        Preconditions.checkAtLeast(permits, 1, "permits");
        this.explicitLimit = true;
        this.gate.setLimit(Math.min(permits, this.maxWorkers));
        this.wakeAll();
    }

    /**
     * Changes the desired number of worker threads at runtime.
     *
     * <p>Growing creates workers immediately, up to the configured maximum. Shrinking only asks idle
     * workers to retire after their current task: a busy worker is never interrupted and leaves when
     * it next becomes idle. The configured {@code minThreads} remains the lower bound.
     *
     * <p>This is the hook an autoscaler or the adaptive controller uses. The worker contract has no
     * separate resize method, so the method lives here rather than on {@link WorkerPool}.
     *
     * @param targetWorkers desired worker count, at least {@code 1}
     */
    public void resize(final int targetWorkers) {
        Preconditions.checkAtLeast(targetWorkers, 1, "targetWorkers");
        final int clamped = Math.clamp(targetWorkers, this.minWorkers, this.maxWorkers);
        final int previous = this.desiredWorkers.getAndSet(clamped);
        this.retirementFloor.set(this.automatic ? this.minWorkers : clamped);
        if (!this.explicitLimit) {
            this.gate.setLimit(clamped);
        }
        if (this.isRunning()) {
            this.spawnUpTo(clamped);
        }
        if (clamped > previous) {
            this.gate.signalAll();
        }
        this.wakeAll();
    }

    /**
     * Returns the current desired worker count.
     *
     * @return the count {@link #start()} and {@link #resize(int)} aim for
     */
    public int desiredWorkers() {
        return this.desiredWorkers.get();
    }

    private boolean isRunning() {
        return this.state.get() == State.RUNNING;
    }

    private void spawnUpTo(final int target) {
        while (!this.isShutdown() && this.liveWorkers.get() < target) {
            synchronized (this.lifecycleLock) {
                if (!this.spawnWorkerLocked()) {
                    return;
                }
            }
        }
    }

    private boolean spawnWorkerLocked() {
        if (this.isShutdown() || this.liveWorkers.get() >= this.maxWorkers) {
            return false;
        }
        int index = -1;
        for (int i = 0; i < this.slots.length; i++) {
            if (this.slots[i] == null) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            return false;
        }
        final Worker worker = new Worker(index);
        final Thread thread;
        try {
            thread = this.threadFactory.newThread(() -> this.runWorker(worker));
            if (thread.isVirtual()) {
                throw new IllegalArgumentException(
                    "DynamicWorkerPool requires platform threads, got a virtual thread from " + this.threadFactory);
            }
        } catch (RuntimeException failure) {
            LOG.log(System.Logger.Level.ERROR, "could not create a worker thread", failure);
            throw failure;
        }
        thread.setUncaughtExceptionHandler((failed, cause) ->
            LOG.log(System.Logger.Level.ERROR, "uncaught exception in " + failed.getName(), cause));
        worker.thread = thread;
        this.slots[index] = worker;
        this.workers.add(worker);
        this.createdThreads.removeIf(dead -> !dead.isAlive());
        this.createdThreads.add(thread);
        this.liveWorkers.incrementAndGet();
        this.peakWorkers = Math.max(this.peakWorkers, this.liveWorkers.get());
        try {
            thread.start();
        } catch (RuntimeException | Error failure) {
            this.workers.remove(worker);
            this.slots[index] = null;
            this.liveWorkers.decrementAndGet();
            LOG.log(System.Logger.Level.ERROR, "could not start a worker thread", failure);
            throw failure;
        }
        return true;
    }

    private void runWorker(final Worker worker) {
        Throwable cause = null;
        boolean exitReserved = false;
        boolean surplusExit = false;
        try {
            this.loop.workerStarted(worker.index);
            long idleSince = this.clock.nanoTime();
            int loopFailures = 0;
            while (!this.isShutdown()) {
                final int retireReason = this.retireReason(idleSince);
                if (retireReason != RETIRE_NONE) {
                    exitReserved = true;
                    surplusExit = retireReason == RETIRE_SURPLUS;
                    break;
                }
                final boolean acquired;
                try {
                    acquired = this.gate.acquire(this.parkNanos(idleSince));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    cause = interrupted;
                    break;
                }
                if (!acquired) {
                    if (this.isShutdown()) {
                        break;
                    }
                    if (idleSince == 0L) {
                        idleSince = this.clock.nanoTime();
                    }
                    continue;
                }
                if (this.isShutdown()) {
                    this.gate.release();
                    break;
                }
                final boolean keepRunning;
                try {
                    keepRunning = this.loop.executeNext(worker.index);
                    loopFailures = 0;
                } catch (Throwable escaped) {
                    this.gate.release();
                    if (escaped instanceof VirtualMachineError) {
                        cause = escaped;
                        break;
                    }
                    loopFailures++;
                    LOG.log(System.Logger.Level.WARNING,
                        "worker " + worker.index + " escaped executeNext (attempt " + loopFailures + ")", escaped);
                    if (loopFailures >= MAX_LOOP_FAILURES) {
                        cause = escaped;
                        break;
                    }
                    idleSince = this.clock.nanoTime();
                    LockSupport.parkNanos(this.parkNanos(idleSince));
                    continue;
                }
                this.gate.release();
                if (!keepRunning) {
                    if (idleSince == 0L) {
                        idleSince = this.clock.nanoTime();
                    }
                    LockSupport.parkNanos(this.parkNanos(idleSince));
                    if (Thread.currentThread().isInterrupted()) {
                        cause = new InterruptedException("worker interrupted while idle");
                        break;
                    }
                } else {
                    idleSince = 0L;
                }
            }
        } catch (Throwable escaped) {
            cause = escaped;
        } finally {
            this.finishWorker(worker, exitReserved, surplusExit, cause);
        }
    }

    private int retireReason(final long idleSinceNanos) {
        final int live = this.liveWorkers.get();
        if (live > this.desiredWorkers.get() && this.reserveExit(this.desiredWorkers.get())) {
            return RETIRE_SURPLUS;
        }
        if (idleSinceNanos == 0L || this.clock.nanoTime() - idleSinceNanos < this.keepAliveNanos) {
            return RETIRE_NONE;
        }
        return this.reserveExit(this.retirementFloor.get()) ? RETIRE_IDLE : RETIRE_NONE;
    }

    private boolean reserveExit(final int floor) {
        synchronized (this.lifecycleLock) {
            if (this.liveWorkers.get() - this.exitReservations.get() <= floor) {
                return false;
            }
            this.exitReservations.incrementAndGet();
            return true;
        }
    }

    private long parkNanos(final long idleSinceNanos) {
        long park = this.idleWaitNanos;
        if (idleSinceNanos != 0L) {
            final long remaining = this.keepAliveNanos - (this.clock.nanoTime() - idleSinceNanos);
            if (remaining > 0L) {
                park = Math.min(park, remaining);
            }
        }
        return Math.max(1L, park);
    }

    private void finishWorker(
        final Worker worker,
        final boolean exitReserved,
        final boolean surplusExit,
        final @Nullable Throwable cause
    ) {
        this.workers.remove(worker);
        try {
            this.loop.workerStopped(worker.index, cause);
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "workerStopped failed for worker " + worker.index, failure);
        }
        synchronized (this.lifecycleLock) {
            if (this.slots[worker.index] == worker) {
                this.slots[worker.index] = null;
            }
            this.liveWorkers.decrementAndGet();
            if (exitReserved) {
                this.exitReservations.decrementAndGet();
            }
        }
        if (surplusExit && this.isRunning() && this.liveWorkers.get() < this.desiredWorkers.get()) {
            try {
                this.spawnUpTo(this.desiredWorkers.get());
            } catch (Throwable failure) {
                LOG.log(System.Logger.Level.ERROR, "could not replace a retired surplus worker", failure);
            }
        }
        this.checkTerminated();
    }

    private void requestShutdown() {
        this.state.getAndUpdate(current ->
            current == State.NEW || current == State.RUNNING ? State.SHUTDOWN : current);
        this.gate.close();
    }

    private void checkTerminated() {
        this.terminationLock.lock();
        try {
            if (this.state.get() == State.SHUTDOWN && this.liveWorkers.get() == 0) {
                this.state.set(State.TERMINATED);
                this.terminated.signalAll();
            }
        } finally {
            this.terminationLock.unlock();
        }
    }

    private void wakeAll() {
        this.gate.signalAll();
        for (final Worker worker : this.workers) {
            final Thread thread = worker.thread;
            if (thread != null) {
                LockSupport.unpark(thread);
            }
        }
    }

    /**
     * One worker thread and the bookkeeping the pool keeps for it.
     */
    private static final class Worker {

        private final int index;
        private volatile Thread thread;

        private Worker(final int index) {
            this.index = index;
        }
    }

    private enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }
}
