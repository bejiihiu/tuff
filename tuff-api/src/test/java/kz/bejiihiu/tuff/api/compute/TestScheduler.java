package kz.bejiihiu.tuff.api.compute;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;

/**
 * Small engine double for the adapter and accessor tests. It records submissions and executes the
 * task body inline, so tests stay deterministic and can assert which thread ran what.
 */
final class TestScheduler implements AsyncComputeScheduler {

    enum FailureMode {
        NONE,
        REJECT,
        SHUTDOWN
    }

    record Submission(String consumerId, ComputeTaskType taskType, ComputePriority priority, ComputeTask<?> task) {
    }

    private final AtomicReference<SchedulerState> state = new AtomicReference<>(SchedulerState.RUNNING);
    private final AtomicReference<FailureMode> failureMode = new AtomicReference<>(FailureMode.NONE);
    private final AtomicLong taskIds = new AtomicLong();
    private final List<Submission> submissions = new CopyOnWriteArrayList<>();
    private final Map<String, ComputeConsumer> consumers = new ConcurrentHashMap<>();

    List<Submission> submissions() {
        return List.copyOf(this.submissions);
    }

    void failWith(final FailureMode mode) {
        this.failureMode.set(Objects.requireNonNull(mode, "mode"));
    }

    @Override
    public <T> CompletableFuture<T> submit(final ComputeTask<T> task) {
        return this.submit(DEFAULT_CONSUMER_ID, ComputeTaskType.GENERAL, ComputePriority.NORMAL, task);
    }

    @Override
    public <T> CompletableFuture<T> submit(final ComputePriority priority, final ComputeTask<T> task) {
        return this.submit(DEFAULT_CONSUMER_ID, ComputeTaskType.GENERAL, priority, task);
    }

    @Override
    public <T> CompletableFuture<T> submit(
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final ComputeTask<T> task
    ) {
        Objects.requireNonNull(task, "task");
        if (this.state.get() == SchedulerState.TERMINATED) {
            throw new ComputeShutdownException("test scheduler is terminated");
        }
        if (this.failureMode.get() == FailureMode.REJECT) {
            throw new ComputeRejectedException(
                ComputeRejectedException.Reason.QUEUE_FULL,
                consumerId,
                priority,
                "test rejection"
            );
        }
        if (this.failureMode.get() == FailureMode.SHUTDOWN) {
            throw new ComputeShutdownException("test scheduler is shutting down");
        }

        this.submissions.add(new Submission(consumerId, taskType, priority, task));
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            future.complete(Objects.requireNonNull(task.execute(), "task returned null"));
        } catch (final Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
        Objects.requireNonNull(request, "request");
        final CompletableFuture<T> future = this.submit(request.consumerId(), request.taskType(), request.priority(), request.task());
        return new Handle<>(this.taskIds.incrementAndGet(), request, future);
    }

    @Override
    public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
        this.consumers.put(consumer.id(), consumer);
        return consumer;
    }

    @Override
    public boolean unregisterConsumer(final String consumerId) {
        return this.consumers.remove(consumerId) != null;
    }

    @Override
    public Optional<ComputeConsumer> consumer(final String consumerId) {
        return Optional.ofNullable(this.consumers.get(consumerId));
    }

    @Override
    public Set<String> consumerIds() {
        return Set.copyOf(this.consumers.keySet());
    }

    @Override
    public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
        final ComputeConsumer stored = this.consumers.get(consumerId);
        if (stored == null) {
            throw new IllegalArgumentException("unknown consumer: " + consumerId);
        }
        this.consumers.put(consumerId, stored.withPolicy(policy));
    }

    @Override
    public SchedulerMetrics metrics() {
        return EmptyMetrics.INSTANCE;
    }

    @Override
    public ComputeSettings settings() {
        return ComputeSettings.defaults();
    }

    @Override
    public SchedulerState state() {
        return this.state.get();
    }

    @Override
    public void start() {
        if (this.state.get() == SchedulerState.TERMINATED) {
            throw new ComputeShutdownException("test scheduler is terminated");
        }
        this.state.set(SchedulerState.RUNNING);
    }

    @Override
    public void shutdown() {
        this.shutdown(Duration.ZERO);
    }

    @Override
    public void shutdown(final Duration gracePeriod) {
        this.state.set(SchedulerState.TERMINATED);
    }

    @Override
    public boolean awaitTermination(final Duration timeout) {
        return this.state.get() == SchedulerState.TERMINATED;
    }

    private static final class Handle<T> implements ComputeTaskHandle<T> {

        private final long taskId;
        private final ComputeRequest<T> request;
        private final CompletableFuture<T> future;

        private Handle(final long taskId, final ComputeRequest<T> request, final CompletableFuture<T> future) {
            this.taskId = taskId;
            this.request = request;
            this.future = future;
        }

        @Override
        public long taskId() {
            return this.taskId;
        }

        @Override
        public String consumerId() {
            return this.request.consumerId();
        }

        @Override
        public ComputeTaskType taskType() {
            return this.request.taskType();
        }

        @Override
        public ComputePriority priority() {
            return this.request.priority();
        }

        @Override
        public Instant submittedAt() {
            return Instant.EPOCH;
        }

        @Override
        public CompletableFuture<T> future() {
            return this.future;
        }

        @Override
        public State state() {
            if (this.future.isCancelled()) {
                return State.CANCELLED;
            }
            if (!this.future.isDone()) {
                return State.QUEUED;
            }
            return this.future.isCompletedExceptionally() ? State.FAILED : State.COMPLETED;
        }

        @Override
        public boolean cancel() {
            return this.future.cancel(false);
        }

        @Override
        public boolean isCancellationRequested() {
            return this.future.isCancelled();
        }

        @Override
        public boolean isCancelled() {
            return this.future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return this.future.isDone();
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
        public Throwable failure() {
            if (!this.future.isCompletedExceptionally()) {
                return null;
            }
            try {
                this.future.join();
                return null;
            } catch (final CompletionException throwable) {
                return throwable.getCause() != null ? throwable.getCause() : throwable;
            } catch (final Throwable throwable) {
                return throwable;
            }
        }
    }

    private enum EmptyMetrics implements SchedulerMetrics {
        INSTANCE;

        @Override
        public Instant capturedAt() {
            return Instant.EPOCH;
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public long submittedTasks() {
            return 0L;
        }

        @Override
        public long completedTasks() {
            return 0L;
        }

        @Override
        public long failedTasks() {
            return 0L;
        }

        @Override
        public long rejectedTasks() {
            return 0L;
        }

        @Override
        public long cancelledTasks() {
            return 0L;
        }

        @Override
        public long expiredTasks() {
            return 0L;
        }

        @Override
        public int queuedTasks() {
            return 0;
        }

        @Override
        public int queueCapacity() {
            return 1;
        }

        @Override
        public int totalWorkers() {
            return 0;
        }

        @Override
        public int activeWorkers() {
            return 0;
        }

        @Override
        public int idleWorkers() {
            return 0;
        }

        @Override
        public int peakWorkers() {
            return 0;
        }

        @Override
        public int concurrencyLimit() {
            return 0;
        }

        @Override
        public double throughputPerSecond() {
            return 0.0d;
        }

        @Override
        public double cpuUtilization() {
            return Double.NaN;
        }

        @Override
        public Duration averageQueueWait() {
            return Duration.ZERO;
        }

        @Override
        public Duration longestQueueWait() {
            return Duration.ZERO;
        }

        @Override
        public Duration averageExecutionTime() {
            return Duration.ZERO;
        }

        @Override
        public Duration longestExecutionTime() {
            return Duration.ZERO;
        }

        @Override
        public Map<String, ConsumerMetrics> consumers() {
            return Map.of();
        }

        @Override
        public Optional<ConsumerMetrics> consumer(final String consumerId) {
            return Optional.empty();
        }
    }
}
