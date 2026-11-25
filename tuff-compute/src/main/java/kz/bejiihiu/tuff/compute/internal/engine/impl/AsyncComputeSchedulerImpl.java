package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.engine.ComputeEngine;

/**
 * The public scheduler facade over {@link ComputeEngine}.
 *
 * <p>Every method is a thin delegation, including the convenience overloads of
 * {@link #submit(ComputeTask)}. Keeping the facade thin means the public API can grow helper methods
 * without touching lifecycle code, and the engine can expose internals (clock, dispatcher, adaptive
 * controller) that must never leak to plugins.
 *
 * <p>{@link #shutdownNow()} is not part of {@link AsyncComputeScheduler} yet; it is available here as
 * a convenience for the server bootstrap that owns the lifecycle.
 */
public final class AsyncComputeSchedulerImpl implements AsyncComputeScheduler {

    private final ComputeEngine engine;

    /**
     * Creates a facade.
     *
     * <p>Use {@link ComputeSchedulers#create(ComputeSettings)} instead of constructing this directly.
     *
     * @param engine engine to delegate to
     */
    AsyncComputeSchedulerImpl(final ComputeEngine engine) {
        this.engine = engine;
    }

    @Override
    public <T> CompletableFuture<T> submit(final ComputeTask<T> task) {
        return this.engine.enqueue(ComputeRequest.of(task)).future();
    }

    @Override
    public <T> CompletableFuture<T> submit(final ComputePriority priority, final ComputeTask<T> task) {
        return this.engine.enqueue(ComputeRequest.of(priority, task)).future();
    }

    @Override
    public <T> CompletableFuture<T> submit(
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final ComputeTask<T> task
    ) {
        return this.engine.enqueue(new ComputeRequest<>(consumerId, taskType, priority, task)).future();
    }

    @Override
    public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
        return this.engine.enqueue(request);
    }

    @Override
    public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
        return this.engine.registerConsumer(consumer);
    }

    @Override
    public boolean unregisterConsumer(final String consumerId) {
        return this.engine.unregisterConsumer(consumerId);
    }

    @Override
    public Optional<ComputeConsumer> consumer(final String consumerId) {
        return this.engine.consumer(consumerId);
    }

    @Override
    public Set<String> consumerIds() {
        return this.engine.consumerIds();
    }

    @Override
    public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
        this.engine.updateConsumerPolicy(consumerId, policy);
    }

    @Override
    public SchedulerMetrics metrics() {
        return this.engine.metrics();
    }

    @Override
    public ComputeSettings settings() {
        return this.engine.settings();
    }

    @Override
    public SchedulerState state() {
        return this.engine.state();
    }

    @Override
    public void start() {
        this.engine.start();
    }

    @Override
    public void shutdown() {
        this.engine.shutdown(this.engine.settings().shutdownGracePeriod());
    }

    @Override
    public void shutdown(final Duration gracePeriod) {
        this.engine.shutdown(gracePeriod);
    }

    /**
     * Stops the scheduler immediately: queued work is cancelled, running work finishes because
     * workers are never interrupted.
     */
    public void shutdownNow() {
        this.engine.shutdownNow();
    }

    @Override
    public void applyProcessorBudget(final int processors) {
        this.engine.applyProcessorBudget(processors);
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        return this.engine.awaitTermination(timeout);
    }

    @Override
    public String toString() {
        return this.engine.toString();
    }
}
