package kz.bejiihiu.tuff.compute.internal.relief.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.RegionRelief;
import kz.bejiihiu.tuff.compute.RegionReliefMetrics;
import kz.bejiihiu.tuff.compute.RegionReliefOwner;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.RegionReliefWork;
import kz.bejiihiu.tuff.compute.RegionReliefWork.Reason;
import kz.bejiihiu.tuff.compute.internal.engine.ComputeEngine;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.jspecify.annotations.Nullable;

/**
 * Default relief path: submit on the owner thread, compute on a worker, apply on the owner thread.
 *
 * <p>The service is deliberately thin around the engine. It owns no queue and no thread; the
 * {@code region-relief} consumer, the bounded queue and the pool belong to the scheduler, and the
 * service only enforces the rules the scheduler cannot know: the submitter must be the owner thread,
 * one owner has a bounded number of requests in flight, a result is applied only while the snapshot
 * epoch is current, and nothing is applied from a thread other than the owner.
 *
 * <p>Every submitted request ends in exactly one terminal call on the owning region thread:
 * {@link RegionReliefWork#apply(Object)} when the result is usable, {@link RegionReliefWork#onFailure}
 * when the computation or the applicator threw, and {@link RegionReliefWork#onDiscard} for stale,
 * expired, cancelled and dropped outcomes. The single exception is an owner port that runs the task
 * off the owner thread: the service then refuses to call anything and counts the request as dropped,
 * because invoking region code from the wrong thread is worse than losing the notification.
 *
 * <p>The in-flight counter and the per-owner slot are released by the same callback that makes the
 * terminal decision, so a request cannot leak a slot and a slot cannot outlive its last request.
 *
 * <p>A failed computation is never rerun inline. The body would almost certainly fail again, and a
 * second failure would cost the region more than the notification it just received.
 */
public final class RegionReliefImpl implements RegionRelief {

    private static final System.Logger LOG = System.getLogger(RegionReliefImpl.class.getName());

    private final ComputeEngine engine;
    private final RegionReliefSettings settings;
    private final ComputeClock clock;
    private final ConcurrentHashMap<RegionReliefOwner, RegionState> owners = new ConcurrentHashMap<>();

    /**
     * Owners that were cancelled at least once.
     *
     * <p>Weak keys on purpose: a region is cancelled when it goes away, and holding it strongly here
     * would leak one small entry per dead region. While the caller still holds the owner (which is the
     * case for any live region), a submission is refused instead of creating a fresh in-flight state.
     */
    private final Set<RegionReliefOwner> cancelledOwners =
        Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    private final AtomicInteger inFlight = new AtomicInteger();

    private final LongAdder requests = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder stale = new LongAdder();
    private final LongAdder inlineFallbacks = new LongAdder();
    private final LongAdder expired = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder savedNanos = new LongAdder();

    /**
     * Creates the relief service of one scheduler.
     *
     * @param engine   engine that receives the offloaded work
     * @param settings relief configuration
     * @param clock    clock used for the saved time estimate
     * @throws NullPointerException if an argument is {@code null}
     */
    public RegionReliefImpl(
        final ComputeEngine engine,
        final RegionReliefSettings settings,
        final ComputeClock clock
    ) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public boolean enabled() {
        return this.settings.enabled() && this.engine.settings().enabled();
    }

    @Override
    public <S, R> boolean submit(final RegionReliefOwner owner, final RegionReliefWork<S, R> work) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(work, "work");
        final Thread caller = Thread.currentThread();
        if (caller != owner.thread()) {
            throw new IllegalStateException(
                "region relief must be submitted from the owning region thread " + owner.thread().getName()
                    + ", not from " + caller.getName()
            );
        }
        this.requests.increment();
        if (owner.isStopped() || this.cancelledOwners.contains(owner)) {
            // the region is gone or was cancelled: reserve nothing, queue nothing, and say so. the
            // callback is safe here because we are on the owner thread
            this.dropped.increment();
            notifyDiscard(work, Reason.DROPPED);
            return false;
        }
        if (!this.enabled() || this.engine.state() != AsyncComputeScheduler.SchedulerState.RUNNING) {
            return this.runInline(owner, work);
        }
        final RegionState state = this.owners.computeIfAbsent(owner, ignored -> new RegionState());
        final boolean admitted;
        synchronized (state) {
            admitted = !state.cancelled && state.inFlight < this.settings.maxInFlightPerRegion();
            if (admitted) {
                state.inFlight++;
            }
        }
        if (!admitted) {
            return this.runInline(owner, work);
        }
        // the snapshot is taken here, on the region thread, and never again
        final SnapshotTask<S, R> task = new SnapshotTask<>(work, work.snapshot(), this.clock);
        final ComputeTaskHandle<R> handle;
        try {
            handle = this.engine.enqueue(new ComputeRequest<>(
                CONSUMER_ID,
                ComputeTaskType.CUSTOM,
                ComputePriority.NORMAL,
                task
            ));
        } catch (ComputeRejectedException | ComputeShutdownException _) {
            this.releaseReservation(state);
            return this.runInline(owner, work);
        }
        this.inFlight.incrementAndGet();
        final Request<S, R> request = new Request<>(owner, work, handle, task);
        final boolean tracked;
        synchronized (state) {
            tracked = !state.cancelled && this.owners.get(owner) == state;
            if (tracked) {
                state.requests.add(request);
            }
        }
        if (tracked) {
            this.accepted.increment();
        } else {
            // the owner was cancelled between the reservation and the submission: nothing counts as
            // accepted, but the future still has to run its cleanup and report the discard
            request.dropped = true;
            handle.cancel();
        }
        handle.future().whenComplete((value, error) -> this.onCompleted(request, value, error));
        return true;
    }

    @Override
    public int cancel(final RegionReliefOwner owner) {
        Objects.requireNonNull(owner, "owner");
        this.cancelledOwners.add(owner);
        final RegionState state = this.owners.remove(owner);
        if (state == null) {
            return 0;
        }
        final Set<Request<?, ?>> pending;
        synchronized (state) {
            state.cancelled = true;
            pending = Set.copyOf(state.requests);
            state.requests.clear();
            state.inFlight = 0;
        }
        for (final Request<?, ?> request : pending) {
            request.dropped = true;
            request.handle.cancel();
        }
        return pending.size();
    }

    @Override
    public RegionReliefMetrics metrics() {
        return new RegionReliefMetrics(
            this.enabled(),
            this.engine.consumer(CONSUMER_ID).isPresent(),
            Math.max(0, this.inFlight.get()),
            this.settings.maxInFlightPerRegion(),
            this.settings.effectiveReservedCores(),
            this.requests.sum(),
            this.accepted.sum(),
            this.completed.sum(),
            this.stale.sum(),
            this.inlineFallbacks.sum(),
            this.expired.sum(),
            this.failed.sum(),
            this.dropped.sum(),
            this.savedNanos.sum()
        );
    }

    /**
     * Gives back the slot reserved before a submission that was refused.
     *
     * @param state owner state the slot was reserved on
     */
    private void releaseReservation(final RegionState state) {
        synchronized (state) {
            state.inFlight = Math.max(0, state.inFlight - 1);
        }
    }

    /**
     * Runs the work on the calling thread, which is the owner thread by contract.
     *
     * @param owner region that owns the work
     * @param work  work to run
     * @param <S>   type of the snapshot
     * @param <R>   type of the computed result
     * @return always {@code false}, because the work did not go to the pool
     */
    private <S, R> boolean runInline(final RegionReliefOwner owner, final RegionReliefWork<S, R> work) {
        if (owner.isStopped()) {
            this.dropped.increment();
            notifyDiscard(work, Reason.DROPPED);
            return false;
        }
        this.inlineFallbacks.increment();
        final R result;
        try {
            result = work.compute(work.snapshot());
        } catch (final RuntimeException failure) {
            this.failed.increment();
            notifyFailure(work, failure);
            return false;
        } catch (final Exception failure) {
            this.failed.increment();
            notifyFailure(work, failure);
            return false;
        }
        if (!owner.isCurrent(work.epoch())) {
            this.stale.increment();
            notifyDiscard(work, Reason.STALE);
            return false;
        }
        try {
            work.apply(result);
        } catch (final RuntimeException failure) {
            this.failed.increment();
            notifyFailure(work, failure);
            return false;
        }
        this.completed.increment();
        return false;
    }

    /**
     * Decides what happens to a finished or cancelled request.
     *
     * @param request request that reached a terminal state
     * @param value   computed value, {@code null} when the task failed or was cancelled
     * @param error   failure of the task, {@code null} on success
     * @param <S>     type of the snapshot
     * @param <R>     type of the computed result
     */
    private <S, R> void onCompleted(
        final Request<S, R> request,
        final @Nullable R value,
        final @Nullable Throwable error
    ) {
        if (error == null) {
            this.dispatch(request, value, Terminal.APPLY, null);
            return;
        }
        final Throwable cause = unwrap(error);
        if (cause instanceof ComputeCancelledException cancelled
            && cancelled.reason() == ComputeCancelledException.Reason.QUEUE_EXPIRED) {
            // the only cancellation the region still needs: the work reruns on the owner thread
            this.dispatch(request, null, Terminal.RERUN_INLINE, null);
            return;
        }
        if (cause instanceof CancellationException) {
            this.dispatch(request, null, Terminal.POOL_CANCELLED, null);
            return;
        }
        this.dispatch(request, null, Terminal.FAILURE, cause);
    }

    /**
     * Schedules the terminal decision back onto the owning region thread.
     *
     * @param request  request whose outcome is known
     * @param value    computed value, {@code null} unless the terminal is an apply
     * @param terminal what has to happen on the owner thread
     * @param failure  failure to report, {@code null} unless the terminal is a failure
     * @param <S>      type of the snapshot
     * @param <R>      type of the computed result
     */
    private <S, R> void dispatch(
        final Request<S, R> request,
        final @Nullable R value,
        final Terminal terminal,
        final @Nullable Throwable failure
    ) {
        try {
            request.owner.runOnOwner(() -> this.finishOnOwner(request, value, terminal, failure));
        } catch (RuntimeException _) {
            // the region refused the task: the outcome cannot be delivered, so it is only counted
            if (request.released.compareAndSet(false, true)) {
                this.dropped.increment();
                this.releaseSlot(request);
            }
        }
    }

    /**
     * Makes the one and only terminal decision for a request, on the owner thread.
     *
     * @param request  request whose outcome is known
     * @param value    computed value, {@code null} unless the terminal is an apply
     * @param terminal what has to happen
     * @param failure  failure to report, {@code null} unless the terminal is a failure
     * @param <S>      type of the snapshot
     * @param <R>      type of the computed result
     */
    private <S, R> void finishOnOwner(
        final Request<S, R> request,
        final @Nullable R value,
        final Terminal terminal,
        final @Nullable Throwable failure
    ) {
        if (!request.released.compareAndSet(false, true)) {
            // a broken owner port scheduled the same terminal twice; only the first one may report
            return;
        }
        try {
            if (Thread.currentThread() != request.owner.thread()) {
                // ownership is absolute: no applicator and no callback ever run off-region
                this.dropped.increment();
                return;
            }
            if (request.dropped) {
                this.dropped.increment();
                notifyDiscard(request.work, Reason.DROPPED);
                return;
            }
            switch (terminal) {
                case FAILURE -> {
                    this.failed.increment();
                    notifyFailure(request.work, failure == null
                        ? new IllegalStateException("region relief computation failed")
                        : failure);
                }
                case POOL_CANCELLED -> {
                    if (request.owner.isStopped()) {
                        this.dropped.increment();
                        notifyDiscard(request.work, Reason.DROPPED);
                    } else {
                        notifyDiscard(request.work, Reason.CANCELLED);
                    }
                }
                case RERUN_INLINE -> this.rerunInline(request);
                case APPLY -> this.applyResult(request, value, request.task.executionNanos);
            }
        } finally {
            this.releaseSlot(request);
        }
    }

    /**
     * Reruns an expired request on the owner thread, which is the only cancellation the region wants
     * back.
     *
     * @param request request that expired in the queue
     * @param <S>     type of the snapshot
     * @param <R>     type of the computed result
     */
    private <S, R> void rerunInline(final Request<S, R> request) {
        if (request.owner.isStopped()) {
            this.expired.increment();
            this.dropped.increment();
            notifyDiscard(request.work, Reason.EXPIRED);
            return;
        }
        this.inlineFallbacks.increment();
        this.expired.increment();
        final R result;
        try {
            result = request.work.compute(request.work.snapshot());
        } catch (final RuntimeException failure) {
            this.failed.increment();
            notifyFailure(request.work, failure);
            return;
        } catch (final Exception failure) {
            this.failed.increment();
            notifyFailure(request.work, failure);
            return;
        }
        this.applyResult(request, result, 0L);
    }

    /**
     * Applies one result, or reports why it was not applied.
     *
     * @param request    request that produced the result
     * @param result     computed value, may be {@code null} only if the body broke its contract
     * @param savedNanos compute time to add to the estimate when the result is applied
     * @param <S>        type of the snapshot
     * @param <R>        type of the computed result
     */
    private <S, R> void applyResult(
        final Request<S, R> request,
        final @Nullable R result,
        final long savedNanos
    ) {
        if (request.owner.isStopped()) {
            this.dropped.increment();
            notifyDiscard(request.work, Reason.DROPPED);
            return;
        }
        if (!request.owner.isCurrent(request.work.epoch())) {
            this.stale.increment();
            notifyDiscard(request.work, Reason.STALE);
            return;
        }
        if (result == null) {
            this.failed.increment();
            notifyFailure(request.work, new NullPointerException("region relief work returned null"));
            return;
        }
        try {
            request.work.apply(result);
        } catch (final RuntimeException failure) {
            this.failed.increment();
            notifyFailure(request.work, failure);
            return;
        }
        this.completed.increment();
        this.savedNanos.add(Math.max(0L, savedNanos));
    }

    /**
     * Reports a failure to the work, from the owner thread.
     *
     * @param work    work that failed
     * @param failure what went wrong
     */
    private static void notifyFailure(final RegionReliefWork<?, ?> work, final Throwable failure) {
        try {
            work.onFailure(failure);
        } catch (final Throwable handlerFailure) {
            LOG.log(System.Logger.Level.WARNING, "region relief failure handler threw", handlerFailure);
        }
    }

    /**
     * Reports a discard to the work, from the owner thread.
     *
     * @param work   work that was discarded
     * @param reason why it was discarded
     */
    private static void notifyDiscard(final RegionReliefWork<?, ?> work, final Reason reason) {
        try {
            work.onDiscard(reason);
        } catch (final Throwable handlerFailure) {
            LOG.log(System.Logger.Level.WARNING, "region relief discard handler threw", handlerFailure);
        }
    }

    /**
     * Releases the request's in-flight slot. Called once per request, after its terminal decision.
     *
     * @param request request that will never be applied again
     */
    private void releaseSlot(final Request<?, ?> request) {
        this.inFlight.decrementAndGet();
        this.owners.computeIfPresent(request.owner, (owner, state) -> {
            synchronized (state) {
                state.requests.remove(request);
                state.inFlight = Math.max(0, state.inFlight - 1);
                return state.inFlight == 0 && !state.cancelled && state.requests.isEmpty() ? null : state;
            }
        });
    }

    private static Throwable unwrap(final Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * What the owner thread has to do with a request whose pool stage is over.
     */
    private enum Terminal {

        /**
         * Apply the computed value.
         */
        APPLY,

        /**
         * Rerun the work inline because the queued request expired.
         */
        RERUN_INLINE,

        /**
         * Report a computation failure.
         */
        FAILURE,

        /**
         * Report a cancellation the pool made.
         */
        POOL_CANCELLED
    }

    /**
     * Per-region bookkeeping. Mutated only while holding the instance monitor, so the in-flight count
     * and the request set cannot drift apart.
     */
    private static final class RegionState {

        private int inFlight;
        private boolean cancelled;
        private final Set<Request<?, ?>> requests = new HashSet<>();
    }

    /**
     * One submitted request. {@link #dropped} is the owner's cancellation flag and {@link #released}
     * makes the terminal decision idempotent.
     *
     * @param <S> type of the snapshot
     * @param <R> type of the computed result
     */
    private static final class Request<S, R> {

        private final RegionReliefOwner owner;
        private final RegionReliefWork<S, R> work;
        private final ComputeTaskHandle<R> handle;
        private final SnapshotTask<S, R> task;
        private volatile boolean dropped;
        private final AtomicBoolean released = new AtomicBoolean();

        private Request(
            final RegionReliefOwner owner,
            final RegionReliefWork<S, R> work,
            final ComputeTaskHandle<R> handle,
            final SnapshotTask<S, R> task
        ) {
            this.owner = owner;
            this.work = work;
            this.handle = handle;
            this.task = task;
        }
    }

    /**
     * The body that runs on a worker: it computes over the snapshot captured on the region thread and
     * records how long it took, which is the estimate the metric reports.
     *
     * @param <S> type of the snapshot
     * @param <R> type of the computed result
     */
    private static final class SnapshotTask<S, R> implements ComputeTask<R> {

        private final RegionReliefWork<S, R> work;
        private final S snapshot;
        private final ComputeClock clock;
        private volatile long executionNanos;

        private SnapshotTask(final RegionReliefWork<S, R> work, final S snapshot, final ComputeClock clock) {
            this.work = work;
            this.snapshot = snapshot;
            this.clock = clock;
        }

        @Override
        public R execute() throws Exception {
            final long startedAt = this.clock.nanoTime();
            try {
                return this.work.compute(this.snapshot);
            } finally {
                this.executionNanos = Math.max(0L, this.clock.nanoTime() - startedAt);
            }
        }
    }
}
