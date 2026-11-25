package kz.bejiihiu.tuff.compute.internal.queue;

import java.time.Duration;
import java.util.Map;

/**
 * Point in time snapshot of a {@link ComputeQueue}.
 *
 * <p>Counters are monotonic for the lifetime of the queue and describe what happened to offers;
 * gauges describe the moment the snapshot was taken. Everything in here is diagnostic, nothing is
 * used to make scheduling decisions.
 *
 * @param capturedAtNanos      monotonic timestamp of the snapshot
 * @param size                 queued tasks across all consumers
 * @param capacity             configured global capacity
 * @param perConsumerSize      immutable map of consumer id to queued task count
 * @param accepted             entries accepted since the queue was created
 * @param rejectedQueueFull    offers refused because of the global capacity
 * @param rejectedConsumerFull offers refused because of a per consumer bound
 * @param evicted              queued entries evicted by {@link kz.bejiihiu.tuff.compute.OverflowPolicy#DROP_OLDEST}
 * @param oldestWaitNanos      wait of the oldest queued entry, or {@code 0} when the queue is empty
 */
public record QueueSnapshot(
    long capturedAtNanos,
    int size,
    int capacity,
    Map<String, Integer> perConsumerSize,
    long accepted,
    long rejectedQueueFull,
    long rejectedConsumerFull,
    long evicted,
    long oldestWaitNanos
) {

    /**
     * Freezes the snapshot maps.
     */
    public QueueSnapshot {
        perConsumerSize = Map.copyOf(perConsumerSize);
    }

    /**
     * Returns how full the queue is.
     *
     * @return a value between {@code 0.0} and {@code 1.0}
     */
    public double utilization() {
        return this.capacity == 0 ? 0.0d : (double) this.size / (double) this.capacity;
    }

    /**
     * Returns the wait of the oldest queued entry as a duration.
     *
     * @return the oldest wait, {@link Duration#ZERO} when the queue is empty
     */
    public Duration oldestWait() {
        return Duration.ofNanos(this.oldestWaitNanos);
    }
}
