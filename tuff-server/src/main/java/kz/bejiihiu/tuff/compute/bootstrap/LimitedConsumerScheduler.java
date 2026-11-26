package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;

/**
 * Scheduler decorator that puts a hard ceiling on dynamic consumer ids and remembers which consumers
 * a plugin registered itself.
 *
 * <p>With {@code allow-dynamic-registration: true} every unknown consumer id from a submission
 * becomes a permanent registry entry with its own queue maps, quota and metrics state. A plugin that
 * derives the id from a player, a world or a request would grow that state without bound, so ids that
 * the scheduler registers on demand are limited to {@link #MAX_DYNAMIC_CONSUMERS} in total. Ids
 * configured in {@code consumers.<id>} do not count and are registered at startup anyway; explicit
 * {@link #registerConsumer(ComputeConsumer)} calls do not count either, because a plugin that names a
 * consumer up front is not the leak this guards against. A refused id never reaches the engine, so a
 * rejection leaves no state behind; unregistering a dynamic consumer frees its slot.
 *
 * <p>The wrapper also records explicit registrations so a configuration reload can tell them apart
 * from the consumers the engine created from the file and leave the plugin policy alone.
 */
final class LimitedConsumerScheduler implements AsyncComputeScheduler {

    /**
     * Maximum number of consumer ids the engine may register on demand, on top of the configured
     * ones. Hard bound, not a tuning knob: it only exists so an id-per-request pattern cannot grow
     * the registry forever. Two hundred fifty six consumers already need tens of thousands of queue
     * and quota entries, and a server with that many real subsystems has other problems.
     */
    static final int MAX_DYNAMIC_CONSUMERS = 256;

    private final AsyncComputeScheduler delegate;
    private final Set<String> configured;
    private final Set<String> explicitlyRegistered = ConcurrentHashMap.newKeySet();

    /**
     * Guards the check-then-register window of the dynamic limit, so two concurrent submissions
     * cannot both take the last slot.
     */
    private final Object admissionLock = new Object();

    private LimitedConsumerScheduler(final AsyncComputeScheduler delegate) {
        this.delegate = delegate;
        this.configured = Set.copyOf(delegate.consumerIds());
    }

    /**
     * Wraps a started scheduler.
     *
     * @param delegate started scheduler that owns the pool
     * @return the guarded view of it
     */
    static LimitedConsumerScheduler wrap(final AsyncComputeScheduler delegate) {
        return new LimitedConsumerScheduler(delegate);
    }

    /**
     * Checks whether a plugin registered this consumer itself.
     *
     * @param consumerId consumer to look up
     * @return {@code true} when the id came from {@link #registerConsumer(ComputeConsumer)}
     */
    boolean isExplicitlyRegistered(final String consumerId) {
        return this.explicitlyRegistered.contains(consumerId);
    }

    /**
     * Counts the ids that were registered on demand and have not been unregistered.
     *
     * @return number of dynamic consumers
     */
    int dynamicConsumerCount() {
        int dynamic = 0;
        for (final String consumerId : this.delegate.consumerIds()) {
            if (!this.configured.contains(consumerId)) {
                dynamic++;
            }
        }
        return dynamic;
    }

    @Override
    public <T> CompletableFuture<T> submit(final ComputeTask<T> task) {
        this.admit(DEFAULT_CONSUMER_ID, ComputePriority.defaultPriority());
        return this.delegate.submit(task);
    }

    @Override
    public <T> CompletableFuture<T> submit(final ComputePriority priority, final ComputeTask<T> task) {
        this.admit(DEFAULT_CONSUMER_ID, priority);
        return this.delegate.submit(priority, task);
    }

    @Override
    public <T> CompletableFuture<T> submit(
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final ComputeTask<T> task
    ) {
        this.admit(consumerId, priority);
        return this.delegate.submit(consumerId, taskType, priority, task);
    }

    @Override
    public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
        this.admit(request.consumerId(), request.priority());
        return this.delegate.enqueue(request);
    }

    @Override
    public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
        final ComputeConsumer stored = this.delegate.registerConsumer(consumer);
        this.explicitlyRegistered.add(stored.id());
        return stored;
    }

    @Override
    public boolean unregisterConsumer(final String consumerId) {
        this.explicitlyRegistered.remove(consumerId);
        return this.delegate.unregisterConsumer(consumerId);
    }

    @Override
    public Optional<ComputeConsumer> consumer(final String consumerId) {
        return this.delegate.consumer(consumerId);
    }

    @Override
    public Set<String> consumerIds() {
        return this.delegate.consumerIds();
    }

    @Override
    public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
        this.delegate.updateConsumerPolicy(consumerId, policy);
    }

    @Override
    public SchedulerMetrics metrics() {
        return this.delegate.metrics();
    }

    @Override
    public ComputeSettings settings() {
        return this.delegate.settings();
    }

    @Override
    public SchedulerState state() {
        return this.delegate.state();
    }

    @Override
    public void start() {
        this.delegate.start();
    }

    @Override
    public void shutdown() {
        this.delegate.shutdown();
    }

    @Override
    public void shutdown(final Duration gracePeriod) {
        this.delegate.shutdown(gracePeriod);
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        return this.delegate.awaitTermination(timeout);
    }

    @Override
    public void applyProcessorBudget(final int processors) {
        this.delegate.applyProcessorBudget(processors);
    }

    /**
     * Refuses a submission for a new dynamic id once the registry is full.
     *
     * @param consumerId consumer the submission is for
     * @param priority   priority the submission is made with, for the rejection
     * @throws ComputeRejectedException if the id is new and the dynamic registry is full
     */
    private void admit(final String consumerId, final ComputePriority priority) {
        if (consumerId.isBlank()) {
            // the engine rejects a blank id with its documented IllegalArgumentException
            return;
        }
        if (this.delegate.consumer(consumerId).isPresent()) {
            return;
        }
        if (!this.delegate.settings().consumers().allowDynamicRegistration()) {
            // the engine rejects unknown ids itself when dynamic registration is off
            return;
        }
        synchronized (this.admissionLock) {
            if (this.delegate.consumer(consumerId).isPresent()) {
                return;
            }
            final int dynamic = this.dynamicConsumerCount();
            if (dynamic >= MAX_DYNAMIC_CONSUMERS) {
                throw new ComputeRejectedException(
                    ComputeRejectedException.Reason.CONSUMER_UNKNOWN,
                    consumerId,
                    priority,
                    "consumer " + consumerId + " is not registered and the engine already holds "
                        + MAX_DYNAMIC_CONSUMERS + " dynamically registered consumer ids; use a configured"
                        + " consumer id or unregister unused ones"
                );
            }
        }
    }
}
