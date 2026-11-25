package kz.bejiihiu.tuff.compute;

import java.util.Objects;

/**
 * A fully specified submission.
 *
 * <p>This is the low level form used by the convenience overloads of
 * {@link AsyncComputeScheduler}. It exists so callers that already know their consumer, task type
 * and priority can validate and pass them around as one value.
 *
 * <p>The optional {@link #label() label} is a human readable name for logs, thread names in dumps,
 * {@link ComputeTaskHandle} diagnostics and flight recordings. It is <b>never</b> part of task
 * identity, ordering, quotas, consumer policy or metrics accounting: two submissions that differ only
 * in their label are indistinguishable to the scheduler. The value is trimmed and truncated to
 * {@value #MAX_LABEL_LENGTH} characters; blank input means "no label" and is stored as the empty
 * string.
 *
 * @param consumerId consumer the task is submitted for
 * @param taskType   declared category of the task
 * @param priority   scheduling priority of the task
 * @param task       the work to execute
 * @param label      optional human readable name, never {@code null}, may be empty
 * @param <T>        type of the produced value
 */
public record ComputeRequest<T>(
    String consumerId,
    ComputeTaskType taskType,
    ComputePriority priority,
    ComputeTask<T> task,
    String label
) {

    /**
     * Maximum number of characters kept from a label.
     */
    public static final int MAX_LABEL_LENGTH = 64;

    /**
     * Validates the request and normalizes the label.
     *
     * @throws NullPointerException     if a required value is {@code null}
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
        label = normalizeLabel(label);
    }

    /**
     * Creates a request without a label.
     *
     * @param consumerId consumer the task is submitted for
     * @param taskType   declared category of the task
     * @param priority   scheduling priority of the task
     * @param task       the work to execute
     */
    public ComputeRequest(
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final ComputeTask<T> task
    ) {
        this(consumerId, taskType, priority, task, "");
    }

    /**
     * Normalizes a label: trims it, treats blank as absent and truncates it softly.
     *
     * @param label raw label, may be {@code null}
     * @return the normalized label, never {@code null}
     */
    public static String normalizeLabel(final String label) {
        if (label == null) {
            return "";
        }
        final String trimmed = label.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        return trimmed.length() <= MAX_LABEL_LENGTH ? trimmed : trimmed.substring(0, MAX_LABEL_LENGTH);
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

    /**
     * Creates a labelled request for the default consumer with {@link ComputeTaskType#GENERAL} and
     * {@link ComputePriority#NORMAL}.
     *
     * @param label human readable name, blank or {@code null} means no label
     * @param task  the work to execute
     * @param <T>   type of the produced value
     * @return the request
     */
    public static <T> ComputeRequest<T> of(final String label, final ComputeTask<T> task) {
        return new ComputeRequest<>(
            AsyncComputeScheduler.DEFAULT_CONSUMER_ID,
            ComputeTaskType.GENERAL,
            ComputePriority.defaultPriority(),
            task,
            label
        );
    }

    /**
     * Creates a labelled request for the default consumer with {@link ComputeTaskType#GENERAL}.
     *
     * @param priority scheduling priority of the task
     * @param label    human readable name, blank or {@code null} means no label
     * @param task     the work to execute
     * @param <T>      type of the produced value
     * @return the request
     */
    public static <T> ComputeRequest<T> of(
        final ComputePriority priority,
        final String label,
        final ComputeTask<T> task
    ) {
        return new ComputeRequest<>(AsyncComputeScheduler.DEFAULT_CONSUMER_ID, ComputeTaskType.GENERAL, priority, task, label);
    }
}
