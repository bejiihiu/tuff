package kz.bejiihiu.tuff.compute;

import java.util.Objects;

/**
 * A fully specified submission.
 *
 * <p>This is the low level form used by the convenience overloads of
 * {@link AsyncComputeScheduler}. It exists so callers that already know their consumer, task type
 * and priority can validate and pass them around as one value.
 *
 * @param consumerId consumer the task is submitted for
 * @param taskType   declared category of the task
 * @param priority   scheduling priority of the task
 * @param task       the work to execute
 * @param <T>        type of the produced value
 */
public record ComputeRequest<T>(
    String consumerId,
    ComputeTaskType taskType,
    ComputePriority priority,
    ComputeTask<T> task
) {

    /**
     * Validates the request.
     *
     * @throws NullPointerException     if any value is {@code null}
     * @throws IllegalArgumentException if the consumer id is blank
     */
    public ComputeRequest {
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(taskType, "taskType");
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(task, "task");
        if (consumerId.isBlank()) {
            throw new IllegalArgumentException("consumerId must not be blank");
        }
    }

    /**
     * Creates a request for the default consumer with {@link ComputeTaskType#GENERAL} and
     * {@link ComputePriority#NORMAL}.
     *
     * @param task the work to execute
     * @param <T>  type of the produced value
     * @return the request
     */
    public static <T> ComputeRequest<T> of(final ComputeTask<T> task) {
        return new ComputeRequest<>(
            AsyncComputeScheduler.DEFAULT_CONSUMER_ID,
            ComputeTaskType.GENERAL,
            ComputePriority.defaultPriority(),
            task
        );
    }

    /**
     * Creates a request for the default consumer with {@link ComputeTaskType#GENERAL}.
     *
     * @param priority scheduling priority of the task
     * @param task     the work to execute
     * @param <T>      type of the produced value
     * @return the request
     */
    public static <T> ComputeRequest<T> of(final ComputePriority priority, final ComputeTask<T> task) {
        return new ComputeRequest<>(AsyncComputeScheduler.DEFAULT_CONSUMER_ID, ComputeTaskType.GENERAL, priority, task);
    }
}
