package kz.bejiihiu.tuff.compute.internal.relief.impl;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
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
import kz.bejiihiu.tuff.compute.internal.engine.ComputeEngine;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.jspecify.annotations.Nullable;

/**
 * Default relief path: submit on the owner thread, compute on a worker, apply on the owner thread.
 *
 * <p>The service is deliberately thin around the engine. It owns no queue and no thread; the
 * {@code region-relief} consumer, the bounded queue and the pool belong to the scheduler, and the
 * service only enforces the four rules the scheduler cannot know: the submitter must be the owner
 * thread, one owner has a bounded number of requests in flight, a result is applied only while the
 * snapshot epoch is current, and the applicator is refused when it would run off the owner thread.
 *
 * <p>Every accepted request is tracked by one {@link Request}. Exactly one of the following happens
 * to it, never two: the applicator runs, the result is dropped, or the request failed. The in-flight
 * counter and the per-owner slot are released in the same callback that makes that decision, so a
 * request cannot leak a slot and a slot cannot outlive its last request.
 *
 * <p>Failure handling is intentionally asymmetric with the inline path: an exception from
 * {@link RegionReliefWork#compute(Object)} on a worker cannot be delivered to the region thread as a
 * throwable without inventing a second callback, so it is counted as {@code failed} and dropped. The
 * inline path runs on the caller's thread and propagates the same exception, which is what the region
 * would have seen before relief existed.
 */
public final class RegionReliefImpl implements RegionRelief {

    private final ComputeEngine engine;
    private final RegionReliefSettings settings;
    private final ComputeClock clock;
    private final ConcurrentHashMap<RegionReliefOwner, RegionState> owners = new ConcurrentHashMap<>();
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
        if (!this.enabled() || this.engine.state() != AsyncComputeScheduler.SchedulerState.RUNNING) {
            return this.runInline(owner, work, false);
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
            return this.runInline(owner, work, false);
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
            return this.runInline(owner, work, false);
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
        if (!tracked) {
            // the owner was cancelled between the reservation and the submission: the result must not
            // be applied, but the future still has to run its cleanup
            request.dropped = true;
            handle.cancel();
        }
        this.accepted.increment();
        handle.future().whenComplete((value, error) -> this.onCompleted(request, value, error));
        return true;
    }

    @Override
    public int cancel(final RegionReliefOwner owner) {
        Objects.requireNonNull(owner, "owner");
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
     * @param owner       region that owns the work
     * @param work        work to run
     * @param afterAccept whether a pool request was accepted and has to be replaced
     * @param <S>         type of the snapshot
     * @param <R>         type of the computed result
     * @return always {@code false}, because the work did not go to the pool
     * @throws IllegalStateException if the inline computation threw a checked exception
     */
    private <S, R> boolean runInline(
        final RegionReliefOwner owner,
        final RegionReliefWork<S, R> work,
        final boolean afterAccept
    ) {
        if (owner.isStopped()) {
            this.dropped.increment();
            return false;
        }
        this.inlineFallbacks.increment();
        if (afterAccept) {
            this.expired.increment();
        }
        final S snapshot = work.snapshot();
        final R result;
        try {
            result = work.compute(snapshot);
        } catch (final RuntimeException failure) {
            this.failed.increment();
            throw failure;
        } catch (final Exception failure) {
            this.failed.increment();
            throw new IllegalStateException("region relief work failed inline", failure);
        }
        if (!owner.isCurrent(work.epoch())) {
            this.stale.increment();
            return false;
        }
        work.apply(result);
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
        if (request.dropped) {
            this.dropped.increment();
            this.release(request);
            return;
        }
        if (error == null) {
            this.dispatch(request, value, false);
            return;
        }
        final Throwable cause = unwrap(error);
        if (cause instanceof CancellationException) {
            // the pool cancelled the request: shutdown, expiry or a consumer change. an expired
            // request is the one case where the region still needs the work done, so it runs inline
            if (cause instanceof ComputeCancelledException cancelled
                && cancelled.reason() == ComputeCancelledException.Reason.QUEUE_EXPIRED) {
                this.dispatch(request, null, true);
                return;
            }
            this.dropped.increment();
            this.release(request);
            return;
        }
        this.failed.increment();
        this.release(request);
    }

    /**
     * Schedules the applicator, or the inline rerun, back onto the owning region thread.
     *
     * @param request request whose result is ready
     * @param value   computed value, {@code null} for an inline rerun
     * @param inline  whether the work has to be computed again on the region thread
     * @param <S>     type of the snapshot
     * @param <R>     type of the computed result
     */
    private <S, R> void dispatch(final Request<S, R> request, final @Nullable R value, final boolean inline) {
        try {
            request.owner.runOnOwner(() -> this.applyOnOwner(request, value, inline));
        } catch (RuntimeException _) {
            // the region refused to take the task: the result cannot be applied and the request is over
            this.dropped.increment();
            this.release(request);
        }
    }

    private <S, R> void applyOnOwner(final Request<S, R> request, final @Nullable R value, final boolean inline) {
        try {
            if (request.dropped || request.owner.isStopped()) {
                this.dropped.increment();
                return;
            }
            if (Thread.currentThread() != request.owner.thread()) {
                // ownership is absolute: a result computed off-region is never applied from here
                this.dropped.increment();
                return;
            }
            final R result;
            if (inline) {
                this.inlineFallbacks.increment();
                this.expired.increment();
                result = this.computeInline(request);
                if (result == null) {
                    return;
                }
            } else {
                result = value;
            }
            if (!request.owner.isCurrent(request.work.epoch())) {
                this.stale.increment();
                return;
            }
            request.work.apply(result);
            this.completed.increment();
            this.savedNanos.add(Math.max(0L, request.task.executionNanos));
        } finally {
            this.release(request);
        }
    }

    private <S, R> @Nullable R computeInline(final Request<S, R> request) {
        try {
            return request.work.compute(request.work.snapshot());
        } catch (final RuntimeException failure) {
            this.failed.increment();
            throw failure;
        } catch (final Exception failure) {
            this.failed.increment();
            throw new IllegalStateException("region relief work failed inline", failure);
        }
    }

    /**
     * Releases the request's in-flight slot exactly once.
     *
     * @param request request that will never be applied again
     */
    private void release(final Request<?, ?> request) {
        if (!request.released.compareAndSet(false, true)) {
            return;
        }
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
     * Per-region bookkeeping. Mutated only while holding the instance monitor, so the in-flight count
     * and the request set cannot drift apart.
     */
    private static final class RegionState {

        private int inFlight;
        private boolean cancelled;
        private final Set<Request<?, ?>> requests = new HashSet<>();
    }

    /**
     * One accepted request. The handle is final; {@link #dropped} is the owner's cancellation flag and
     * {@link #released} makes the slot release idempotent.
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
