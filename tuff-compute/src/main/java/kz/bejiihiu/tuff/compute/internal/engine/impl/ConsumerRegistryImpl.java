package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.ConsumerRegistry;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;

/**
 * Concurrent map based consumer registry.
 *
 * <p>Deliberately dumb: it stores immutable {@link ComputeConsumer} values and nothing else. The
 * engine combines a write here with a policy update in the queue and the quota controller; keeping
 * the registry free of those side effects makes the update order explicit and testable.
 */
final class ConsumerRegistryImpl implements ConsumerRegistry {

    private final Map<String, ComputeConsumer> consumers = new ConcurrentHashMap<>();

    @Override
    public ComputeConsumer register(final ComputeConsumer consumer) {
        Preconditions.requireNonNull(consumer, "consumer");
        this.consumers.put(consumer.id(), consumer);
        return consumer;
    }

    @Override
    public Optional<ComputeConsumer> unregister(final String consumerId) {
        Preconditions.requireNonBlank(consumerId, "consumerId");
        return Optional.ofNullable(this.consumers.remove(consumerId));
    }

    @Override
    public Optional<ComputeConsumer> find(final String consumerId) {
        Preconditions.requireNonBlank(consumerId, "consumerId");
        return Optional.ofNullable(this.consumers.get(consumerId));
    }

    @Override
    public Set<ComputeConsumer> all() {
        return Set.copyOf(this.consumers.values());
    }

    @Override
    public Set<String> ids() {
        return Set.copyOf(this.consumers.keySet());
    }

    @Override
    public ComputeConsumer updatePolicy(final String consumerId, final ConsumerPolicy policy) {
        Preconditions.requireNonBlank(consumerId, "consumerId");
        Preconditions.requireNonNull(policy, "policy");
        final ComputeConsumer updated = this.consumers.computeIfPresent(
            consumerId,
            (ignored, current) -> current.withPolicy(policy)
        );
        if (updated == null) {
            throw new IllegalArgumentException("consumer " + consumerId + " is not registered");
        }
        return updated;
    }

    @Override
    public int size() {
        return this.consumers.size();
    }
}
