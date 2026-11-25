package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import org.jspecify.annotations.Nullable;

/**
 * Minimal handle used by the queue entry in tests. It is not a scheduler replacement; it only keeps
 * the cancellation flag and the future that the contracts require.
 */
final class TestTaskHandle implements ComputeTaskHandle<Object> {

    private final long taskId;
    private final String consumerId;
    private final Instant submittedAt = Instant.now();
    private final CompletableFuture<Object> future = new CompletableFuture<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.QUEUED);
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private volatile @Nullable Throwable failure;

    TestTaskHandle(final long taskId, final String consumerId) {
        this.taskId = taskId;
        this.consumerId = consumerId;
    }

    void markRunning() {
        this.state.compareAndSet(State.QUEUED, State.RUNNING);
    }

    void markCompleted() {
        if (this.state.compareAndSet(State.RUNNING, State.COMPLETED)) {
            this.future.complete(null);
        }
    }

    void markFailed(final Throwable thrown) {
        this.failure = thrown;
        this.state.set(State.FAILED);
        this.future.completeExceptionally(thrown);
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
        return ComputeTaskType.GENERAL;
    }

    @Override
    public ComputePriority priority() {
        return ComputePriority.NORMAL;
    }

    @Override
    public Instant submittedAt() {
        return this.submittedAt;
    }

    @Override
    public CompletableFuture<Object> future() {
        return this.future;
    }

    @Override
    public State state() {
        return this.state.get();
    }

    @Override
    public boolean cancel() {
        if (!this.cancellationRequested.compareAndSet(false, true)) {
            return false;
        }
        while (true) {
            final State current = this.state.get();
            if (current.isTerminal()) {
                return false;
            }
            if (this.state.compareAndSet(current, State.CANCELLED)) {
                this.future.completeExceptionally(
                    new ComputeCancelledException(this.taskId, ComputeCancelledException.Reason.CALLER, "cancelled"));
                return true;
            }
        }
    }

    @Override
    public boolean isCancellationRequested() {
        return this.cancellationRequested.get();
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
        return -1L;
    }

    @Override
    public long executionNanos() {
        return -1L;
    }

    @Override
    public @Nullable Throwable failure() {
        return this.failure;
    }
}
