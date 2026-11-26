package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.jspecify.annotations.Nullable;

/**
 * Fixed size ring of the most recently finished tasks.
 *
 * <p>{@link #record} runs on whatever thread terminalized a task — a worker, the queue janitor, a
 * submitting thread during eviction — so it does not allocate, does not take a lock and never
 * waits for a reader. Each slot is published with a small sequence stamp: a writer claims a slot
 * with a compare and set, writes its plain fields and bumps the stamp twice, a reader accepts the
 * fields only when it saw the same even stamp before and after the copy. A slot that is already
 * being written is skipped and the next one is tried, so two terminal events can never interleave
 * into one slot.
 *
 * <p>The labels and consumer ids stored here are references owned by the task handles, the ring
 * never copies or interns them. Only {@link #slowest} allocates, and only when a snapshot is
 * requested.
 */
public final class RecentTaskRing {

    /**
     * Number of terminal events the ring remembers.
     */
    public static final int CAPACITY = 64;

    private final Slot[] slots = new Slot[CAPACITY];
    private final AtomicLong sequence = new AtomicLong();

    /**
     * Creates an empty ring.
     */
    public RecentTaskRing() {
        for (int index = 0; index < this.slots.length; index++) {
            this.slots[index] = new Slot();
        }
    }

    /**
     * Records one terminal event.
     *
     * <p>Tasks that never started are ignored: a queued cancellation has no lane and no execution
     * time, so it would only push a real measurement out of the ring. When every slot is being
     * written at the same instant the entry is dropped, which is acceptable for a diagnostic
     * sample of the last {@value #CAPACITY} completions.
     *
     * @param taskId         unique task id
     * @param consumerId     consumer the task belonged to
     * @param taskType       declared category of the task
     * @param priority       submitted priority
     * @param lane           execution lane, {@code null} for a task that never started
     * @param label          normalized label, may be empty, never {@code null}
     * @param executionNanos how long the body ran, negative when it never started
     * @param queueWaitNanos how long the task waited before it started
     * @param state          terminal state
     */
    public void record(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final @Nullable String lane,
        final String label,
        final long executionNanos,
        final long queueWaitNanos,
        final ComputeTaskHandle.State state
    ) {
        if (lane == null || executionNanos < 0L) {
            return;
        }
        final long ticket = this.sequence.getAndIncrement();
        for (int attempt = 0; attempt < this.slots.length; attempt++) {
            final Slot slot = this.slots[(int) ((ticket + attempt) % this.slots.length)];
            if (!slot.tryClaim()) {
                continue;
            }
            try {
                slot.write(taskId, consumerId, taskType, priority, lane, label, executionNanos,
                    queueWaitNanos, state);
            } finally {
                slot.release();
            }
            return;
        }
    }

    /**
     * Returns every retained entry, sorted by execution time descending.
     *
     * @return an immutable snapshot of the ring, at most {@value #CAPACITY} entries
     */
    public List<SchedulerMetrics.CompletedTaskInfo> snapshot() {
        final List<SchedulerMetrics.CompletedTaskInfo> entries = new ArrayList<>(this.slots.length);
        for (final Slot slot : this.slots) {
            final SchedulerMetrics.CompletedTaskInfo entry = slot.read();
            if (entry != null) {
                entries.add(entry);
            }
        }
        entries.sort((first, second) -> {
            final int byExecution = Long.compare(second.executionNanos(), first.executionNanos());
            return byExecution != 0 ? byExecution : Long.compare(second.taskId(), first.taskId());
        });
        return List.copyOf(entries);
    }

    /**
     * Returns at most {@code limit} entries, slowest first.
     *
     * @param limit maximum number of entries, values below one yield an empty list
     * @return the sorted and limited entries
     */
    public List<SchedulerMetrics.CompletedTaskInfo> slowest(final int limit) {
        if (limit <= 0) {
            return List.of();
        }
        final List<SchedulerMetrics.CompletedTaskInfo> entries = this.snapshot();
        return entries.size() <= limit ? entries : List.copyOf(entries.subList(0, limit));
    }

    /**
     * One ring slot.
     *
     * <p>Fields are plain because the stamp is the publication barrier: a reader copies them
     * between two reads of the volatile stamp and rejects the copy when the stamp moved or was odd.
     * Only one writer owns a slot at a time, which is what keeps the two writes monotonic.
     */
    private static final class Slot {

        private final AtomicInteger busy = new AtomicInteger();

        private volatile long stamp;
        private long taskId;
        private String consumerId = "";
        private ComputeTaskType taskType = ComputeTaskType.GENERAL;
        private ComputePriority priority = ComputePriority.NORMAL;
        private String lane = "";
        private String label = "";
        private long executionNanos;
        private long queueWaitNanos;
        private ComputeTaskHandle.State state = ComputeTaskHandle.State.COMPLETED;

        /**
         * Takes ownership of the slot without blocking.
         *
         * @return {@code true} when this caller may write the slot
         */
        boolean tryClaim() {
            return this.busy.compareAndSet(0, 1);
        }

        /**
         * Gives the slot back after a write.
         */
        void release() {
            this.busy.set(0);
        }

        /**
         * Publishes one entry.
         *
         * @param taskId         unique task id
         * @param consumerId     consumer the task belonged to
         * @param taskType       declared category of the task
         * @param priority       submitted priority
         * @param lane           execution lane
         * @param label          normalized label, may be empty
         * @param executionNanos how long the body ran
         * @param queueWaitNanos how long the task waited before it started
         * @param state          terminal state
         */
        void write(
            final long taskId,
            final String consumerId,
            final ComputeTaskType taskType,
            final ComputePriority priority,
            final String lane,
            final String label,
            final long executionNanos,
            final long queueWaitNanos,
            final ComputeTaskHandle.State state
        ) {
            final long before = this.stamp;
            // odd stamps mark a write in progress; any monotonically growing value is fine
            this.stamp = before + 1L;
            this.taskId = taskId;
            this.consumerId = consumerId;
            this.taskType = taskType;
            this.priority = priority;
            this.lane = lane;
            this.label = label;
            this.executionNanos = executionNanos;
            this.queueWaitNanos = queueWaitNanos;
            this.state = state;
            this.stamp = before + 2L;
        }

        /**
         * Reads one entry, or returns {@code null} when the slot is empty or kept changing.
         *
         * @return the entry, or {@code null}
         */
        SchedulerMetrics.@Nullable CompletedTaskInfo read() {
            for (int attempt = 0; attempt < 4; attempt++) {
                final long before = this.stamp;
                if (before == 0L) {
                    return null;
                }
                if ((before & 1L) != 0L) {
                    continue;
                }
                final long entryTaskId = this.taskId;
                final String entryConsumerId = this.consumerId;
                final ComputeTaskType entryTaskType = this.taskType;
                final ComputePriority entryPriority = this.priority;
                final String entryLane = this.lane;
                final String entryLabel = this.label;
                final long entryExecutionNanos = this.executionNanos;
                final long entryQueueWaitNanos = this.queueWaitNanos;
                final ComputeTaskHandle.State entryState = this.state;
                if (before == this.stamp) {
                    return new SchedulerMetrics.CompletedTaskInfo(
                        entryTaskId,
                        entryConsumerId,
                        entryTaskType,
                        entryPriority,
                        entryLane,
                        entryLabel.isEmpty() ? Optional.empty() : Optional.of(entryLabel),
                        entryExecutionNanos,
                        entryQueueWaitNanos,
                        entryState
                    );
                }
            }
            return null;
        }
    }
}
