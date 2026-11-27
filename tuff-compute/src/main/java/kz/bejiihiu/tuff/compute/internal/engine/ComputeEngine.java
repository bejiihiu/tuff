package kz.bejiihiu.tuff.compute.internal.engine;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.RegionRelief;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Internal orchestrator of the scheduler.
 *
 * <p>{@link kz.bejiihiu.tuff.compute.AsyncComputeScheduler} is a thin facade over this interface;
 * keeping the two apart means the public API can grow convenience methods without touching the
 * lifecycle, and the engine can expose internals (the clock, the dispatcher) that must not leak.
 *
 * <p>Lifecycle rules that every implementation must keep:
 * <ul>
 *     <li>a {@code NEW} engine accepts no submissions and has no threads;</li>
 *     <li>{@link #start()} is idempotent and moves {@code NEW} to {@code RUNNING};</li>
 *     <li>{@link #shutdown(Duration)} is idempotent, refuses new submissions and eventually reaches
 *     {@code TERMINATED};</li>
 *     <li>a {@code TERMINATED} engine never goes back to {@code RUNNING}.</li>
 * </ul>
 */
public interface ComputeEngine {

    /**
     * Starts the engine.
     *
     * @throws IllegalStateException if the engine is already terminated
     */
    void start();

    /**
     * Starts a graceful shutdown.
     *
     * @param gracePeriod how long running tasks may still finish, must not be negative
     */
    void shutdown(Duration gracePeriod);

    /**
     * Starts an immediate shutdown, cancelling queued work.
     */
    void shutdownNow();

    /**
     * Applies a new processor budget to an automatically sized pool.
     *
     * <p>No-op for a fixed worker count or when the engine is not running. Never interrupts running
     * tasks and never touches the queue.
     *
     * @param processors processors available to the engine, at least {@code 1}
     */
    void applyProcessorBudget(int processors);

    /**
     * Waits until the engine reaches the terminated state.
     *
     * @param timeout maximum time to wait, must not be negative
     * @return {@code true} if the engine terminated within the timeout
     * @throws InterruptedException if the calling thread was interrupted while waiting
     */
    boolean awaitTermination(Duration timeout) throws InterruptedException;

    /**
     * Returns the current lifecycle state.
     *
     * @return the state
     */
    AsyncComputeScheduler.SchedulerState state();

    /**
     * Validates, queues and returns a handle for a submission.
     *
     * @param request the fully specified submission
     * @param <T>     type of the produced value
     * @return the handle of the accepted task
     */
    <T> ComputeTaskHandle<T> enqueue(ComputeRequest<T> request);

    /**
     * Requests cancellation of a task by id.
     *
     * @param taskId id of the task
     * @return {@code true} if the task was found and moved to the cancelled state
     */
    boolean cancel(long taskId);

    /**
     * Registers or replaces a consumer.
     *
     * @param consumer consumer to register
     * @return the consumer as stored
     */
    ComputeConsumer registerConsumer(ComputeConsumer consumer);

    /**
     * Unregisters a consumer and cancels its queued work.
     *
     * @param consumerId consumer to unregister
     * @return {@code true} if it was registered
     */
    boolean unregisterConsumer(String consumerId);

    /**
     * Replaces the policy of a registered consumer.
     *
     * @param consumerId consumer to update
     * @param policy     replacement policy
     */
    void updateConsumerPolicy(String consumerId, ConsumerPolicy policy);

    /**
     * Looks up a registered consumer.
     *
     * @param consumerId consumer to look up
     * @return the consumer, or an empty optional
     */
    Optional<ComputeConsumer> consumer(String consumerId);

    /**
     * Returns the ids of all registered consumers.
     *
     * @return an immutable snapshot
     */
    Set<String> consumerIds();

    /**
     * Returns a scheduler metrics snapshot.
     *
     * @return the snapshot
     */
    SchedulerMetrics metrics();

    /**
     * Returns the configuration of the engine.
     *
     * @return the effective settings
     */
    ComputeSettings settings();

    /**
     * Returns the clock every component of this engine uses.
     *
     * @return the clock
     */
    ComputeClock clock();

    /**
     * Returns the region relief path of this engine.
     *
     * <p>The service exists before the engine starts and keeps working while the engine is disabled;
     * submissions then run inline.
     *
     * @return the relief service, never {@code null}
     */
    RegionRelief regionRelief();
}
