package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Central entry point for background CPU work in Tuff.
 *
 * <p>One scheduler owns one pool of worker threads. Subsystems register themselves as
 * {@link ComputeConsumer consumers}, submit {@link ComputeTask tasks} with a priority and get a
 * future back. The scheduler decides when the task runs, on which worker, and how the pool is shared
 * between consumers; a consumer never creates its own executor.
 *
 * <p>Every submission is accepted, queued and executed at most once. A task that fails is not
 * retried automatically, and a task that is cancelled is not executed afterwards. Queue capacity is
 * bounded, so overflow is refused with {@link ComputeRejectedException} instead of growing the heap
 * without limit.
 *
 * <p>Threading: all methods are safe to call from any thread and never block for long. Submission
 * never waits for a worker slot; it either enqueues, or refuses, and returns. Task bodies run on
 * scheduler workers and must not touch mutable world state, region owned objects or entities.
 *
 * <p>Lifecycle: a fresh scheduler is in {@link SchedulerState#NEW} and accepts no work until
 * {@link #start()} is called. After {@link #shutdown()} the scheduler stops claiming new tasks,
 * finishes or cancels what is queued depending on the grace period, and moves to
 * {@link SchedulerState#TERMINATED}. Terminated schedulers stay terminated.
 */
public interface AsyncComputeScheduler {

    /**
     * Id of the consumer that receives submissions which do not name one.
     *
     * <p>The scheduler registers this consumer on start with the default policy from
     * {@link ComputeSettings.Consumers#defaultPolicy()}.
     */
    String DEFAULT_CONSUMER_ID = "default";

    /**
     * Submits a task under the default consumer, {@link ComputeTaskType#GENERAL} and
     * {@link ComputePriority#NORMAL}.
     *
     * <p>This is a convenience overload of
     * {@link #submit(String, ComputeTaskType, ComputePriority, ComputeTask)} and shares its
     * semantics.
     *
     * @param task the work to execute
     * @param <T>  type of the produced value
     * @return a future completed with the task result
     * @throws ComputeRejectedException if the submission is refused, for example because the queue is
     *     full or the default consumer is disabled
     * @throws ComputeShutdownException if the scheduler is not running
     * @throws NullPointerException     if {@code task} is {@code null}
     */
    <T> CompletableFuture<T> submit(ComputeTask<T> task);

    /**
     * Submits a task under the default consumer and {@link ComputeTaskType#GENERAL}.
     *
     * <p>This is a convenience overload of
     * {@link #submit(String, ComputeTaskType, ComputePriority, ComputeTask)} and shares its
     * semantics.
     *
     * @param priority scheduling priority of the task
     * @param task     the work to execute
     * @param <T>      type of the produced value
     * @return a future completed with the task result
     * @throws ComputeRejectedException if the submission is refused
     * @throws ComputeShutdownException if the scheduler is not running
     * @throws NullPointerException     if an argument is {@code null}
     */
    <T> CompletableFuture<T> submit(ComputePriority priority, ComputeTask<T> task);

    /**
     * Submits a task.
     *
     * <p>The task is validated against the consumer's limits, given a unique id, queued and
     * eventually executed exactly once. The returned future completes with the value the task
     * produced, or exceptionally with the throwable it threw. Cancellation completes it with
     * {@link ComputeCancelledException}.
     *
     * <p>Ordering is not guaranteed beyond priority and fairness: two tasks with the same priority
     * of the same consumer are dispatched roughly in submission order, but concurrent workers may
     * finish them in any order.
     *
     * <p>The call does not block waiting for capacity. When the global or per consumer queue is full
     * the consumer's {@link OverflowPolicy} decides whether the oldest task is evicted or the
     * submission is refused.
     *
     * @param consumerId consumer the task is submitted for
     * @param taskType   declared category of the task
     * @param priority   scheduling priority of the task
     * @param task       the work to execute
     * @param <T>        type of the produced value
     * @return a future completed with the task result
     * @throws ComputeRejectedException if the submission is refused, see
     *     {@link ComputeRejectedException.Reason}
     * @throws ComputeShutdownException if the scheduler is not running
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if {@code consumerId} is blank
     */
    <T> CompletableFuture<T> submit(
        String consumerId,
        ComputeTaskType taskType,
        ComputePriority priority,
        ComputeTask<T> task
    );

    /**
     * Submits a task and returns its handle.
     *
     * <p>Semantics are identical to
     * {@link #submit(String, ComputeTaskType, ComputePriority, ComputeTask)}, but the caller also
     * gets a handle it can use to inspect progress and request cooperative cancellation.
     *
     * @param request the fully specified submission
     * @param <T>     type of the produced value
     * @return the handle of the accepted task
     * @throws ComputeRejectedException if the submission is refused
     * @throws ComputeShutdownException if the scheduler is not running
     * @throws NullPointerException     if {@code request} is {@code null}
     */
    <T> ComputeTaskHandle<T> enqueue(ComputeRequest<T> request);

    /**
     * Registers a consumer, or replaces the policy of an already registered one.
     *
     * <p>Registration is idempotent by id: registering the same id twice replaces the stored
     * consumer and its policy, which makes repeated plugin enable/disable cycles safe. A consumer
     * that is newly registered with a disabled policy is stored but refuses submissions until its
     * policy is enabled again.
     *
     * @param consumer consumer to register
     * @return the consumer as stored by the scheduler
     * @throws ComputeShutdownException if the scheduler is terminated
     * @throws NullPointerException     if {@code consumer} is {@code null}
     */
    ComputeConsumer registerConsumer(ComputeConsumer consumer);

    /**
     * Unregisters a consumer.
     *
     * <p>Queued tasks of that consumer are cancelled with
     * {@link ComputeCancelledException.Reason#CONSUMER_DISABLED}; tasks that are already running are
     * only flagged as cancellation requested and are allowed to finish, because workers are never
     * interrupted. New submissions for the id are handled as if the consumer had never existed.
     *
     * @param consumerId consumer to unregister
     * @return {@code true} if a consumer with that id was registered
     * @throws NullPointerException if {@code consumerId} is {@code null}
     */
    boolean unregisterConsumer(String consumerId);

    /**
     * Looks up a registered consumer.
     *
     * @param consumerId consumer to look up
     * @return the consumer, or an empty optional if it is not registered
     * @throws NullPointerException if {@code consumerId} is {@code null}
     */
    Optional<ComputeConsumer> consumer(String consumerId);

    /**
     * Returns the ids of all registered consumers.
     *
     * @return an immutable snapshot of the registered ids
     */
    Set<String> consumerIds();

    /**
     * Replaces the policy of a registered consumer.
     *
     * <p>Already queued tasks are kept. The new limits apply from the moment this method returns:
     * lower concurrency limits do not interrupt running tasks, they just prevent new ones from
     * starting until the consumer is below its limit again.
     *
     * @param consumerId consumer to update
     * @param policy     replacement policy
     * @throws IllegalArgumentException if no consumer with that id is registered
     * @throws NullPointerException     if an argument is {@code null}
     */
    void updateConsumerPolicy(String consumerId, ConsumerPolicy policy);

    /**
     * Returns a snapshot of the scheduler metrics.
     *
     * @return the current metrics snapshot
     */
    SchedulerMetrics metrics();

    /**
     * Returns the configuration this scheduler was created with.
     *
     * @return the effective settings, immutable
     */
    ComputeSettings settings();

    /**
     * Returns the current lifecycle state.
     *
     * @return the state, never {@code null}
     */
    SchedulerState state();

    /**
     * Checks whether the scheduler is running and accepting work.
     *
     * @return {@code true} if the state is {@link SchedulerState#RUNNING}
     */
    default boolean isRunning() {
        return this.state() == SchedulerState.RUNNING;
    }

    /**
     * Starts the scheduler.
     *
     * <p>Creates the worker pool, applies the consumer configuration and begins dispatching. Calling
     * this on a running scheduler has no effect.
     *
     * @throws ComputeShutdownException if the scheduler was already terminated
     */
    void start();

    /**
     * Shuts the scheduler down gracefully.
     *
     * <p>Equivalent to {@link #shutdown(Duration)} with
     * {@link ComputeSettings#shutdownGracePeriod()}. Idempotent: the first call starts the shutdown
     * and every later call only waits for it to finish.
     */
    void shutdown();

    /**
     * Shuts the scheduler down gracefully, allowing running tasks to finish.
     *
     * <p>New submissions are refused with {@link ComputeShutdownException}, no new task is claimed
     * from the queue, and running tasks are given at most {@code gracePeriod} to finish. Queued tasks
     * that did not start within the grace period are cancelled with
     * {@link ComputeCancelledException.Reason#SHUTDOWN}, then the workers are stopped. Calling this
     * while the scheduler is in {@link SchedulerState#NEW} moves it straight to
     * {@link SchedulerState#TERMINATED} without starting anything.
     *
     * <p>This method does not block; use {@link #awaitTermination(Duration)} to wait.
     *
     * @param gracePeriod how long running tasks may still finish, must not be negative
     * @throws IllegalArgumentException if {@code gracePeriod} is negative
     */
    void shutdown(Duration gracePeriod);

    /**
     * Waits for the scheduler to reach {@link SchedulerState#TERMINATED}.
     *
     * @param timeout maximum time to wait, must not be negative
     * @return {@code true} if the scheduler terminated within the timeout
     * @throws InterruptedException     if the calling thread was interrupted while waiting
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    boolean awaitTermination(Duration timeout) throws InterruptedException;

    /**
     * Lifecycle state of a scheduler.
     */
    enum SchedulerState {

        /**
         * Created but not started. No work is accepted and no worker exists.
         */
        NEW,

        /**
         * Running and accepting work.
         */
        RUNNING,

        /**
         * Shutting down. Work is no longer accepted, running tasks are finishing or being cancelled.
         */
        SHUTTING_DOWN,

        /**
         * Stopped for good. Nothing is accepted and no worker exists.
         */
        TERMINATED
    }
}
