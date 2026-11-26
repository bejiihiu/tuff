package kz.bejiihiu.tuff.compute;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * A registered producer of compute work.
 *
 * <p>Every subsystem that wants worker time registers itself as a consumer. The consumer carries
 * its identity, the task types and priorities it is allowed to use and its {@link ConsumerPolicy}.
 * The scheduler uses the id for quotas, fairness and metrics, so ids must be stable, unique and
 * lowercase-ish; they are shown in metrics and logs as is.
 *
 * <p>Consumers are immutable values. To change limits at runtime use
 * {@link AsyncComputeScheduler#updateConsumerPolicy(String, ConsumerPolicy)} instead of registering
 * the consumer again.
 *
 * @param id                unique consumer id, never blank
 * @param displayName       human readable name for logs and metrics, never blank
 * @param allowedTaskTypes  task types this consumer may submit, never empty
 * @param maxPriority       highest priority this consumer may submit
 * @param policy            limits and fairness hints for this consumer
 */
public record ComputeConsumer(
    String id,
    String displayName,
    Set<ComputeTaskType> allowedTaskTypes,
    ComputePriority maxPriority,
    ConsumerPolicy policy
) {

    /**
     * Validates and freezes the consumer.
     *
     * <p>A disabled policy is legal and does not block registration: the consumer exists and keeps
     * its metrics, but every submission is rejected with
     * {@link ComputeRejectedException.Reason#CONSUMER_DISABLED} until the policy is enabled again.
     *
     * @throws IllegalArgumentException if the id or display name is blank, or no task type is
     *     allowed
     * @throws NullPointerException if any argument is {@code null}
     */
    public ComputeConsumer {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(displayName, "displayName");
        Objects.requireNonNull(allowedTaskTypes, "allowedTaskTypes");
        Objects.requireNonNull(maxPriority, "maxPriority");
        Objects.requireNonNull(policy, "policy");
        if (id.isBlank()) {
            throw new IllegalArgumentException("consumer id must not be blank");
        }
        if (displayName.isBlank()) {
            throw new IllegalArgumentException("displayName must not be blank");
        }
        if (allowedTaskTypes.isEmpty()) {
            throw new IllegalArgumentException("consumer " + id + " must allow at least one task type");
        }
        allowedTaskTypes = Set.copyOf(allowedTaskTypes);
    }

    /**
     * Creates a consumer that may submit every task type at any priority.
     *
     * @param id     unique consumer id
     * @param policy limits and fairness hints
     * @return the consumer
     */
    public static ComputeConsumer of(final String id, final ConsumerPolicy policy) {
        return new ComputeConsumer(id, id, Set.of(ComputeTaskType.values()), ComputePriority.CRITICAL, policy);
    }

    /**
     * Creates a consumer with explicit limits.
     *
     * @param id               unique consumer id
     * @param allowedTaskTypes task types this consumer may submit
     * @param maxPriority      highest priority this consumer may submit
     * @param policy           limits and fairness hints
     * @return the consumer
     */
    public static ComputeConsumer of(
        final String id,
        final Collection<ComputeTaskType> allowedTaskTypes,
        final ComputePriority maxPriority,
        final ConsumerPolicy policy
    ) {
        return new ComputeConsumer(id, id, Set.copyOf(allowedTaskTypes), maxPriority, policy);
    }

    /**
     * Returns a copy of this consumer with a different policy and the same identity.
     *
     * @param newPolicy replacement policy
     * @return a consumer with the same id, name, task types and priority, and the given policy
     */
    public ComputeConsumer withPolicy(final ConsumerPolicy newPolicy) {
        return new ComputeConsumer(this.id, this.displayName, this.allowedTaskTypes, this.maxPriority, newPolicy);
    }

    /**
     * Checks whether this consumer may submit the given task type and priority.
     *
     * @param taskType type of the task
     * @param priority priority of the task
     * @return {@code true} if the type is allowed and the priority does not outrank
     *     {@link #maxPriority()}
     */
    public boolean allows(final ComputeTaskType taskType, final ComputePriority priority) {
        return this.allowedTaskTypes.contains(taskType) && !priority.outranks(this.maxPriority);
    }
}
