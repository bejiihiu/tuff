package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.util.Objects;
import jdk.jfr.EventType;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.internal.metrics.JfrRecorder;
import kz.bejiihiu.tuff.compute.internal.metrics.event.AgingActivatedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.QueueDepthSampleEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskCancelledEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskCompletedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskFailedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskQueueExpiredEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskRejectedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskStartedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.TaskSubmittedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.WorkerPoolResizedEvent;
import kz.bejiihiu.tuff.compute.internal.metrics.event.WorkerPoolStateEvent;
import org.jspecify.annotations.Nullable;

/**
 * {@link JfrRecorder} backed by the JDK flight recorder.
 *
 * <p>Cost when nothing records the scheduler is one {@link EventType#isEnabled()} check per hook: the
 * event object is only allocated after that check passes, and no string is built on the hot path
 * before it. There is no extra throttling on top of JFR's own {@code threshold} setting, because a
 * hard coded rate limit would make a recording silently biased; operators who need less data use the
 * recording configuration, where the decision belongs.
 *
 * <p>On a runtime that does not ship {@code jdk.jfr} the constructor still succeeds: the recorder
 * probes the module once in an isolated holder class and degrades to a silent no-op, so a server
 * build never fails to start because profiling is unavailable.
 */
public final class FlightJfrRecorder implements JfrRecorder {

    private final @Nullable Types types;
    private volatile int lastTotalWorkers = -1;

    /**
     * Creates a recorder. Never throws, even when the flight recorder module is absent.
     */
    public FlightJfrRecorder() {
        this.types = Types.load();
    }

    @Override
    public boolean enabled() {
        final Types current = this.types;
        return current != null && current.anyEnabled();
    }

    @Override
    public void taskQueued(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final int queueDepth
    ) {
        if (this.types == null || !Types.SUBMITTED.isEnabled()) {
            return;
        }
        try {
            final TaskSubmittedEvent event = new TaskSubmittedEvent();
            event.taskId = taskId;
            event.consumerId = consumerId;
            event.taskType = taskType.name();
            event.priority = priority.name();
            event.queueDepth = queueDepth;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    @Override
    public void taskStarted(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final long queueWaitNanos,
        final int activeTasks
    ) {
        if (this.types == null || !Types.STARTED.isEnabled()) {
            return;
        }
        try {
            final TaskStartedEvent event = new TaskStartedEvent();
            event.taskId = taskId;
            event.consumerId = consumerId;
            event.taskType = taskType.name();
            event.priority = priority.name();
            event.queueWaitNanos = queueWaitNanos;
            event.activeTasks = activeTasks;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    @Override
    public void taskFinished(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final long executionNanos,
        final @Nullable Throwable failure
    ) {
        if (this.types == null) {
            return;
        }
        if (failure == null) {
            if (!Types.COMPLETED.isEnabled()) {
                return;
            }
            try {
                final TaskCompletedEvent event = new TaskCompletedEvent();
                event.taskId = taskId;
                event.consumerId = consumerId;
                event.taskType = taskType.name();
                event.priority = priority.name();
                event.executionNanos = executionNanos;
                event.commit();
            } catch (Throwable _) {
                // profiling must never break a task
            }
            return;
        }
        if (!Types.FAILED.isEnabled()) {
            return;
        }
        try {
            final TaskFailedEvent event = new TaskFailedEvent();
            event.taskId = taskId;
            event.consumerId = consumerId;
            event.taskType = taskType.name();
            event.priority = priority.name();
            event.executionNanos = executionNanos;
            event.failureType = failure.getClass().getName();
            event.failureMessage = failure.getMessage();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    @Override
    public void taskCancelled(final long taskId, final String consumerId, final ComputeCancelledException.Reason reason) {
        if (this.types == null) {
            return;
        }
        if (reason == ComputeCancelledException.Reason.QUEUE_EXPIRED) {
            if (!Types.QUEUE_EXPIRED.isEnabled()) {
                return;
            }
            try {
                final TaskQueueExpiredEvent event = new TaskQueueExpiredEvent();
                event.taskId = taskId;
                event.consumerId = consumerId;
                event.commit();
            } catch (Throwable _) {
                // profiling must never break a task
            }
            return;
        }
        if (!Types.CANCELLED.isEnabled()) {
            return;
        }
        try {
            final TaskCancelledEvent event = new TaskCancelledEvent();
            event.taskId = taskId;
            event.consumerId = consumerId;
            event.reason = reason.name();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    @Override
    public void queueState(final int queuedTasks, final int capacity) {
        if (this.types == null || !Types.QUEUE_SAMPLE.isEnabled()) {
            return;
        }
        try {
            final QueueDepthSampleEvent event = new QueueDepthSampleEvent();
            event.queuedTasks = queuedTasks;
            event.capacity = capacity;
            event.fillRatio = capacity <= 0 ? 0.0d : (double) queuedTasks / (double) capacity;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    @Override
    public void workerState(final int totalWorkers, final int activeWorkers, final int idleWorkers, final int concurrencyLimit) {
        if (this.types == null) {
            return;
        }
        if (Types.WORKER_SAMPLE.isEnabled()) {
            try {
                final WorkerPoolStateEvent event = new WorkerPoolStateEvent();
                event.totalWorkers = totalWorkers;
                event.activeWorkers = activeWorkers;
                event.idleWorkers = idleWorkers;
                event.concurrencyLimit = concurrencyLimit;
                event.commit();
            } catch (Throwable _) {
                // profiling must never break a task
            }
        }
        final int previous = this.lastTotalWorkers;
        this.lastTotalWorkers = totalWorkers;
        if (previous < 0 || previous == totalWorkers || !Types.WORKER_RESIZED.isEnabled()) {
            return;
        }
        try {
            final WorkerPoolResizedEvent event = new WorkerPoolResizedEvent();
            event.totalWorkers = totalWorkers;
            event.concurrencyLimit = concurrencyLimit;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    /**
     * Emits a rejection event.
     *
     * <p>The frozen {@link JfrRecorder} contract has no rejection hook, so this method is called by
     * {@link ComputeMetricsCollector} when it owns a recorder of this concrete type. It is public so
     * a future engine can also emit the event directly on the rejection path.
     *
     * @param consumerId consumer whose submission was refused
     * @param reason     why it was refused
     */
    public void taskRejected(final String consumerId, final ComputeRejectedException.Reason reason) {
        if (this.types == null || !Types.REJECTED.isEnabled()) {
            return;
        }
        try {
            final TaskRejectedEvent event = new TaskRejectedEvent();
            event.consumerId = consumerId;
            event.reason = reason.name();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    /**
     * Emits an aging event.
     *
     * <p>Not part of the frozen recorder contract; the dispatcher calls it once it reports aging
     * decisions. Keeping the method here means no metric code has to change when that happens.
     *
     * @param consumerId   consumer of the aged task
     * @param fromPriority priority the task was submitted with
     * @param toPriority   effective priority after the aging step
     * @param waitedNanos  how long the task had been waiting
     */
    public void agingActivated(
        final String consumerId,
        final ComputePriority fromPriority,
        final ComputePriority toPriority,
        final long waitedNanos
    ) {
        if (this.types == null || !Types.AGING.isEnabled()) {
            return;
        }
        try {
            final AgingActivatedEvent event = new AgingActivatedEvent();
            event.consumerId = consumerId;
            event.fromPriority = Objects.requireNonNull(fromPriority, "fromPriority").name();
            event.toPriority = Objects.requireNonNull(toPriority, "toPriority").name();
            event.waitedNanos = waitedNanos;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a task
        }
    }

    /**
     * Lazily loaded holder for the flight recorder types.
     *
     * <p>It exists so that a JVM without {@code jdk.jfr} fails while loading this nested class, which
     * the constructor catches, instead of failing while loading {@link FlightJfrRecorder} itself.
     */
    private static final class Types {

        static final EventType SUBMITTED = EventType.getEventType(TaskSubmittedEvent.class);
        static final EventType STARTED = EventType.getEventType(TaskStartedEvent.class);
        static final EventType COMPLETED = EventType.getEventType(TaskCompletedEvent.class);
        static final EventType FAILED = EventType.getEventType(TaskFailedEvent.class);
        static final EventType CANCELLED = EventType.getEventType(TaskCancelledEvent.class);
        static final EventType QUEUE_EXPIRED = EventType.getEventType(TaskQueueExpiredEvent.class);
        static final EventType REJECTED = EventType.getEventType(TaskRejectedEvent.class);
        static final EventType QUEUE_SAMPLE = EventType.getEventType(QueueDepthSampleEvent.class);
        static final EventType WORKER_SAMPLE = EventType.getEventType(WorkerPoolStateEvent.class);
        static final EventType WORKER_RESIZED = EventType.getEventType(WorkerPoolResizedEvent.class);
        static final EventType AGING = EventType.getEventType(AgingActivatedEvent.class);

        private Types() {
        }

        static @Nullable Types load() {
            try {
                return new Types();
            } catch (Throwable _) {
                // the runtime has no flight recorder; every hook becomes a no-op
                return null;
            }
        }

        boolean anyEnabled() {
            return STARTED.isEnabled() || SUBMITTED.isEnabled() || COMPLETED.isEnabled();
        }
    }
}
