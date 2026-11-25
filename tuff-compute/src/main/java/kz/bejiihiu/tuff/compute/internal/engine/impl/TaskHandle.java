package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import org.jspecify.annotations.Nullable;

/**
 * The real task handle owned by the engine.
 *
 * <p>This class is the single authority over a task's lifecycle. It owns one
 * {@link AtomicReference} state, completes exactly one real {@link CompletableFuture} and tells the
 * engine when it reached a terminal state. Everything else in the engine (dispatcher, janitor,
 * eviction listener, shutdown) only asks the handle to transition; none of them completes a future
 * on its own.
 *
 * <p>Why the state machine lives here and not in the dispatcher: cancellation can arrive through
 * four different threads (the caller's {@code cancel()}, the caller's {@code future().cancel(true)},
 * the queue janitor, the offering thread inside a drop-oldest eviction) and racing paths must not
 * double complete a future or count a task twice. One compare and set covers all of them.
 *
 * <p>The future is a {@link CompletableFuture} subclass so dependent stages, {@code thenApply},
 * {@code thenAccept}, {@code join} and {@code get} behave exactly like a normal future. Only
 * {@link HandleFuture#cancel(boolean)} is overridden: it routes to {@link #cancel()} instead of
 * completing the future with a bare {@link java.util.concurrent.CancellationException}, because a
 * queued task has to leave the queue and a running task has to be flagged, not interrupted.
 *
 * @param <T> type of the produced value
 */
final class TaskHandle<T> implements ComputeTaskHandle<T> {

    /**
     * The engine side of a handle: it acts on cancellation requests and is told when the handle
     * ends.
     *
     * <p>Implemented by the engine, which owns the queue, the dispatcher and the live handle map.
     */
    interface Owner {

        /**
         * Applies a cancellation request that the handle has already flagged.
         *
         * @param handle handle that received the request
         * @return {@code true} if the request is effective, {@code false} if the task already ended
         */
        boolean requestCancel(TaskHandle<?> handle);

        /**
         * Called exactly once when the handle reaches a terminal state.
         *
         * @param handle handle that ended
         */
        void onTerminal(TaskHandle<?> handle);
    }

    private final long taskId;
    private final String consumerId;
    private final ComputeTaskType taskType;
    private final ComputePriority priority;
    private final Instant submittedAt;
    private final long submittedAtNanos;
    private final CompletableFuture<T> future;
    private final Owner owner;

    private final AtomicReference<State> state = new AtomicReference<>(State.QUEUED);
    private final AtomicBoolean cancelRequested = new AtomicBoolean();
    private final AtomicBoolean terminalSignalled = new AtomicBoolean();

    private volatile @Nullable QueueEntry entry;
    private volatile ComputeCancelledException.@Nullable Reason cancelReason;
    private volatile @Nullable Throwable failure;
    private volatile long queueWaitNanos = -1L;
    private volatile long executionNanos = -1L;

    /**
     * Creates a queued handle.
     *
     * @param taskId           unique task id
     * @param consumerId       consumer the task belongs to
     * @param taskType         declared task category
     * @param priority         submitted priority
     * @param submittedAt      wall clock instant of acceptance
     * @param submittedAtNanos monotonic acceptance timestamp
     * @param owner            engine side of the handle
     */
    TaskHandle(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final Instant submittedAt,
        final long submittedAtNanos,
        final Owner owner
    ) {
        this.taskId = taskId;
        this.consumerId = consumerId;
        this.taskType = taskType;
        this.priority = priority;
        this.submittedAt = submittedAt;
        this.submittedAtNanos = submittedAtNanos;
        this.owner = owner;
        this.future = new HandleFuture();
    }

    /**
     * Attaches the queue entry that represents this handle.
     *
     * <p>Set once, before the entry is offered to the queue, so the engine can remove the entry from
     * the queue when cancellation arrives.
     *
     * @param queueEntry entry created for this handle
     */
    void bindEntry(final QueueEntry queueEntry) {
        this.entry = queueEntry;
    }

    /**
     * Returns the queue entry of this handle.
     *
     * @return the entry, or {@code null} before the handle was bound
     */
    @Nullable QueueEntry entry() {
        return this.entry;
    }

    /**
     * Returns the monotonic timestamp used to compute the queue wait.
     *
     * @return acceptance timestamp in nanoseconds
     */
    long submittedAtNanos() {
        return this.submittedAtNanos;
    }

    /**
     * Tries to move this task from queued to running.
     *
     * @return {@code true} if the worker owns the execution, {@code false} if the task was already
     *     cancelled or finished
     */
    boolean markRunning() {
        if (this.cancelRequested.get()) {
            return false;
        }
        return this.state.compareAndSet(State.QUEUED, State.RUNNING);
    }

    /**
     * Records how long the task waited before it started.
     *
     * @param nanos queue wait in nanoseconds
     */
    void recordStart(final long nanos) {
        this.queueWaitNanos = nanos;
    }

    /**
     * Records how long the task body ran.
     *
     * @param nanos execution time in nanoseconds
     */
    void recordExecution(final long nanos) {
        this.executionNanos = nanos;
    }

    /**
     * How {@link #finish(Object, Throwable)} ended a running task.
     */
    enum Completion {

        /**
         * The task produced a value.
         */
        COMPLETED,

        /**
         * The task threw.
         */
        FAILED,

        /**
         * The task was cancelled and its result, if any, was discarded.
         */
        CANCELLED
    }

    /**
     * Completes a running task with a result or a failure.
     *
     * <p>The returned value tells the caller how to account the terminal event. {@code null} means
     * the task was already flagged for cancellation and the caller must complete and account it as
     * cancelled; {@link Completion#CANCELLED} means this call won a late cancellation race and the
     * caller only has to emit the cancellation event, because the future is already completed.
     *
     * @param result    value produced by the task, {@code null} when {@code throwable} is set
     * @param throwable failure thrown by the task, or {@code null} on success
     * @return the outcome, or {@code null} when the caller must run the cancellation path
     */
    @SuppressWarnings("unchecked")
    @Nullable Completion finish(final @Nullable Object result, final @Nullable Throwable throwable) {
        if (this.cancelRequested.get()) {
            return null;
        }
        final State target = throwable == null ? State.COMPLETED : State.FAILED;
        if (!this.state.compareAndSet(State.RUNNING, target)) {
            return null;
        }
        if (this.cancelRequested.get() && this.state.compareAndSet(target, State.CANCELLED)) {
            // a cancel slipped in after the terminal CAS; the worker thread still owns the
            // accounting, so report the task as cancelled instead of delivering the result.
            this.future.completeExceptionally(this.cancelledException());
            this.signalTerminal();
            return Completion.CANCELLED;
        }
        if (throwable == null) {
            this.future.complete((T) result);
            this.signalTerminal();
            return Completion.COMPLETED;
        }
        this.failure = throwable;
        this.future.completeExceptionally(throwable);
        this.signalTerminal();
        return Completion.FAILED;
    }

    /**
     * Completes a task that was cancelled before it produced a result.
     *
     * <p>Safe to call from any thread and any number of times: only the caller that wins the
     * terminal transition completes the future and reports success.
     *
     * @param reason why the task was cancelled
     * @return {@code true} if this call moved the handle into the cancelled state
     */
    boolean completeCancelled(final ComputeCancelledException.Reason reason) {
        this.rememberCancelReason(reason);
        while (true) {
            final State current = this.state.get();
            if (current.isTerminal()) {
                return false;
            }
            if (this.state.compareAndSet(current, State.CANCELLED)) {
                this.future.completeExceptionally(this.cancelledException());
                this.signalTerminal();
                return true;
            }
        }
    }

    /**
     * Flags an already accepted cancellation without completing the handle.
     *
     * <p>Used for tasks that are running or already claimed by a worker: the worker notices the flag
     * between the body and the completion and reports the cancellation itself, which keeps the
     * metrics thread affinity correct.
     *
     * @param reason why the task is being cancelled
     * @return {@code true} if the flag was not set before
     */
    boolean requestCooperativeCancel(final ComputeCancelledException.Reason reason) {
        this.rememberCancelReason(reason);
        return this.cancelRequested.compareAndSet(false, true);
    }

    /**
     * Marks a handle whose submission was refused. It never has a visible future to complete.
     */
    void reject() {
        this.state.set(State.REJECTED);
        this.signalTerminal();
    }

    /**
     * Returns why cancellation was requested.
     *
     * @return the reason, or {@link ComputeCancelledException.Reason#CALLER} when unknown
     */
    ComputeCancelledException.Reason cancelReason() {
        final ComputeCancelledException.Reason reason = this.cancelReason;
        return reason == null ? ComputeCancelledException.Reason.CALLER : reason;
    }

    /**
     * Signals the engine exactly once that this handle reached a terminal state.
     */
    private void signalTerminal() {
        if (this.terminalSignalled.compareAndSet(false, true)) {
            this.owner.onTerminal(this);
        }
    }

    private void rememberCancelReason(final ComputeCancelledException.Reason reason) {
        if (this.cancelReason == null) {
            this.cancelReason = reason;
        }
    }

    private ComputeCancelledException cancelledException() {
        return new ComputeCancelledException(
            this.taskId,
            this.cancelReason(),
            "compute task " + this.taskId + " of consumer " + this.consumerId + " was cancelled"
        );
    }

    @Override
    public long taskId() {
        return this.taskId;
    }

    @Override
    public String consumerId() {
        return this.consumerId;
    }

    @Override
    public ComputeTaskType taskType() {
        return this.taskType;
    }

    @Override
    public ComputePriority priority() {
        return this.priority;
    }

    @Override
    public Instant submittedAt() {
        return this.submittedAt;
    }

    @Override
    public CompletableFuture<T> future() {
        return this.future;
    }

    @Override
    public State state() {
        return this.state.get();
    }

    @Override
    public boolean cancel() {
        if (this.state.get().isTerminal()) {
            return false;
        }
        if (!this.cancelRequested.compareAndSet(false, true)) {
            return false;
        }
        this.rememberCancelReason(ComputeCancelledException.Reason.CALLER);
        final boolean effective = this.owner.requestCancel(this);
        if (!effective) {
            // the task ended while the request was in flight; the cancellation was not accepted.
            this.cancelRequested.compareAndSet(true, false);
        }
        return effective;
    }

    @Override
    public boolean isCancellationRequested() {
        return this.cancelRequested.get();
    }

    @Override
    public boolean isCancelled() {
        return this.state.get() == State.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return this.state.get().isTerminal();
    }

    @Override
    public long queueWaitNanos() {
        return this.queueWaitNanos;
    }

    @Override
    public long executionNanos() {
        return this.executionNanos;
    }

    @Override
    public @Nullable Throwable failure() {
        return this.failure;
    }

    @Override
    public String toString() {
        return "ComputeTaskHandle[task=" + this.taskId
            + ", consumer=" + this.consumerId
            + ", type=" + this.taskType
            + ", priority=" + this.priority
            + ", state=" + this.state.get() + ']';
    }

    /**
     * Result future whose {@link #cancel(boolean)} is routed back to the handle.
     */
    private final class HandleFuture extends CompletableFuture<T> {

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            // compute cancellation is cooperative and never interrupts; the boolean is ignored on
            // purpose, exactly like the handle contract says.
            return TaskHandle.this.cancel();
        }
    }
}
