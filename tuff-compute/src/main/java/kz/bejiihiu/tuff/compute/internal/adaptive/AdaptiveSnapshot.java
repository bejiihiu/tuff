package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Point in time snapshot of an {@link AdaptiveConcurrencyController}.
 *
 * <p>Useful for logs and for explaining why the pool suddenly shrank: the averages next to the
 * limits are the numbers the last evaluation saw.
 *
 * @param capturedAt           wall clock instant of the snapshot
 * @param enabled              whether adaptive concurrency was enabled
 * @param currentLimit         limit in force when the snapshot was taken
 * @param desiredLimit         limit the controller would apply next
 * @param minLimit             configured minimum
 * @param maxLimit             configured maximum
 * @param samples              number of task samples currently in the window
 * @param averageQueueWait     average queue wait of the samples in the window
 * @param averageExecution     average execution time of the samples in the window
 * @param estimatedUtilization estimated utilisation derived from the samples, {@code 0.0} when unknown
 */
public record AdaptiveSnapshot(
    Instant capturedAt,
    boolean enabled,
    int currentLimit,
    int desiredLimit,
    int minLimit,
    int maxLimit,
    int samples,
    Duration averageQueueWait,
    Duration averageExecution,
    double estimatedUtilization
) {

    /**
     * Validates the snapshot.
     *
     * @throws NullPointerException if a reference is {@code null}
     */
    public AdaptiveSnapshot {
        Objects.requireNonNull(capturedAt, "capturedAt");
        Objects.requireNonNull(averageQueueWait, "averageQueueWait");
        Objects.requireNonNull(averageExecution, "averageExecution");
    }

    /**
     * Checks whether the controller wants to change the limit.
     *
     * @return {@code true} if {@link #desiredLimit()} differs from {@link #currentLimit()}
     */
    public boolean wantsChange() {
        return this.desiredLimit != this.currentLimit;
    }
}
