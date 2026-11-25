package kz.bejiihiu.tuff.compute.internal.engine;

import java.util.Optional;
import java.util.Set;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;

/**
 * Registry of known consumers.
 *
 * <p>Keeps the mapping from consumer id to the immutable {@link ComputeConsumer} value. The registry
 * is deliberately dumb: it does not enforce policies, does not touch queues and does not fire
 * metrics. Those three things belong to the engine, which combines a lookup here with a policy
 * update in the queue, the quota controller and the metrics collector, in that order.
 *
 * <p>Threading: safe for concurrent reads, registration and removal. A read never blocks on a
 * registration; implementations should use a concurrent map and immutable values.
 */
public interface ConsumerRegistry {

    /**
     * Registers a consumer, replacing any consumer with the same id.
     *
     * @param consumer consumer to store
     * @return the consumer as stored
     */
    ComputeConsumer register(ComputeConsumer consumer);

    /**
     * Removes a consumer.
     *
     * @param consumerId consumer to remove
     * @return the removed consumer, or an empty optional if it was not registered
     */
    Optional<ComputeConsumer> unregister(String consumerId);

    /**
     * Looks up a consumer.
     *
     * @param consumerId consumer to look up
     * @return the consumer, or an empty optional
     */
    Optional<ComputeConsumer> find(String consumerId);

    /**
     * Returns all registered consumers.
     *
     * @return an immutable snapshot of the registered consumers, keyed by id
     */
    Set<ComputeConsumer> all();

    /**
     * Returns the ids of all registered consumers.
     *
     * @return an immutable snapshot of the ids
     */
    Set<String> ids();

    /**
     * Replaces the policy of a registered consumer.
     *
     * @param consumerId consumer to update
     * @param policy     replacement policy
     * @return the updated consumer
     * @throws IllegalArgumentException if the consumer is not registered
     */
    ComputeConsumer updatePolicy(String consumerId, ConsumerPolicy policy);

    /**
     * Returns how many consumers are registered.
     *
     * @return the number of registered consumers
     */
    int size();
}
