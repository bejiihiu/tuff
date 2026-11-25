package kz.bejiihiu.tuff.api.compute;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTaskType;

/**
 * An {@link Executor} that submits work to the Tuff compute engine.
 *
 * <p>Many libraries and server APIs accept only a {@link java.util.concurrent.Executor}. This
 * adapter lets them use the shared compute pool without knowing about the engine: every
 * {@link #execute(Runnable)} is submitted under a fixed consumer id, task type and priority.
 *
 * <pre>{@code
 * final Executor executor = new ComputeExecutor(compute, "map-colour-cache", ComputePriority.LOW);
 * cache.rebuildAsync(executor);
 * }</pre>
 *
 * <p>A task body runs on a compute worker and therefore must not touch world state. The adapter
 * reports failures of the task body to its failure handler; it never lets them escape into the
 * engine or the caller.
 *
 * <p>Rejections are normal: the queue may be full, the consumer may be disabled, or the engine may
 * be shutting down. {@link RejectionPolicy} decides what happens then. {@link #shutdown()} only
 * detaches this adapter; it never shuts the shared engine down.
 */
public final class ComputeExecutor implements Executor {

    private static final System.Logger LOGGER = System.getLogger("TuffCompute");

    private final AsyncComputeScheduler scheduler;
    private final String consumerId;
    private final ComputePriority priority;
    private final ComputeTaskType taskType;
    private final RejectionPolicy rejectionPolicy;
    private final Consumer<? super Throwable> failureHandler;
    private final AtomicBoolean shutdown = new AtomicBoolean();

    /**
     * Creates an executor with {@link ComputePriority#NORMAL}, {@link ComputeTaskType#GENERAL},
     * {@link RejectionPolicy#CALLER_RUNS} and a failure handler that logs to the system logger.
     *
     * @param scheduler  engine to submit to
     * @param consumerId consumer the work is submitted for
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if {@code consumerId} is blank
     */
    public ComputeExecutor(final AsyncComputeScheduler scheduler, final String consumerId) {
        this(
            scheduler,
            consumerId,
            ComputePriority.defaultPriority(),
            ComputeTaskType.GENERAL,
            RejectionPolicy.CALLER_RUNS,
            logFailure()
        );
    }

    /**
     * Creates an executor with {@link ComputeTaskType#GENERAL} and a failure handler that logs to
     * the system logger.
     *
     * @param scheduler       engine to submit to
     * @param consumerId      consumer the work is submitted for
     * @param priority        priority used for every submission
     * @param rejectionPolicy what to do when the engine refuses a submission
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if {@code consumerId} is blank
     */
    public ComputeExecutor(
        final AsyncComputeScheduler scheduler,
        final String consumerId,
        final ComputePriority priority,
        final RejectionPolicy rejectionPolicy
    ) {
        this(scheduler, consumerId, priority, ComputeTaskType.GENERAL, rejectionPolicy, logFailure());
    }

    /**
     * Creates a fully specified executor.
     *
     * @param scheduler       engine to submit to
     * @param consumerId      consumer the work is submitted for
     * @param priority        priority used for every submission
     * @param taskType        task category used for every submission
     * @param rejectionPolicy what to do when the engine refuses a submission
     * @param failureHandler  handler for throwables produced by task bodies
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if {@code consumerId} is blank
     */
    public ComputeExecutor(
        final AsyncComputeScheduler scheduler,
        final String consumerId,
        final ComputePriority priority,
        final ComputeTaskType taskType,
        final RejectionPolicy rejectionPolicy,
        final Consumer<? super Throwable> failureHandler
    ) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.consumerId = Objects.requireNonNull(consumerId, "consumerId");
        if (consumerId.isBlank()) {
            throw new IllegalArgumentException("consumerId must not be blank");
        }
        this.priority = Objects.requireNonNull(priority, "priority");
        this.taskType = Objects.requireNonNull(taskType, "taskType");
        this.rejectionPolicy = Objects.requireNonNull(rejectionPolicy, "rejectionPolicy");
        this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    }

    /**
     * Submits a command to the compute engine.
     *
     * <p>The command returns no value, so the adapter submits a task that produces a shared marker
     * value; the engine contract requires a non-null result. If the engine refuses the submission,
     * the configured {@link RejectionPolicy} is applied and this method does not throw. If the
     * engine is already shutting down, the command is dropped: there is no safe context left to run
     * it in.
     *
     * <p>The command may run on a worker thread or, with caller-runs, directly on the calling
     * thread. It must never touch mutable world state.
     *
     * @param command the work to execute
     * @throws NullPointerException      if {@code command} is {@code null}
     * @throws RejectedExecutionException if this adapter was {@linkplain #shutdown() shut down}
     */
    @Override
    public void execute(final Runnable command) {
        Objects.requireNonNull(command, "command");
        if (this.shutdown.get()) {
            throw new RejectedExecutionException("ComputeExecutor for consumer '" + this.consumerId + "' was shut down");
        }

        final CompletableFuture<Boolean> result;
        try {
            result = this.scheduler.submit(this.consumerId, this.taskType, this.priority, () -> {
                command.run();
                return Boolean.TRUE;
            });
        } catch (ComputeRejectedException _) {
            if (this.rejectionPolicy == RejectionPolicy.CALLER_RUNS) {
                command.run();
            }
            return;
        } catch (ComputeShutdownException _) {
            // the engine is going away; dropping is the only safe option, throwing would break
            // callers that use this adapter as a plain Executor during shutdown
            return;
        }

        result.whenComplete((ignored, failure) -> {
            if (failure == null) {
                return;
            }
            try {
                this.failureHandler.accept(failure);
            } catch (final Throwable throwable) {
                LOGGER.log(System.Logger.Level.ERROR, "The compute executor failure handler threw", throwable);
            }
        });
    }

    /**
     * Detaches this adapter without touching the shared engine.
     *
     * <p>Already submitted work keeps running and is still reported to the failure handler. Every
     * later {@link #execute(Runnable)} throws {@link RejectedExecutionException}, as the
     * {@link Executor} contract expects. This method is idempotent. The engine itself stays running
     * for every other consumer until the server shuts it down.
     */
    public void shutdown() {
        this.shutdown.set(true);
    }

    /**
     * Checks whether this adapter was detached with {@link #shutdown()}.
     *
     * @return {@code true} if new commands are rejected
     */
    public boolean isShutdown() {
        return this.shutdown.get();
    }

    /**
     * Returns the consumer id every command is submitted for.
     *
     * @return the consumer id
     */
    public String consumerId() {
        return this.consumerId;
    }

    /**
     * Returns the priority used for every submission.
     *
     * @return the compute priority
     */
    public ComputePriority priority() {
        return this.priority;
    }

    /**
     * Returns the task category used for every submission.
     *
     * @return the compute task type
     */
    public ComputeTaskType taskType() {
        return this.taskType;
    }

    /**
     * Returns the policy applied when the engine refuses a submission.
     *
     * @return the rejection policy
     */
    public RejectionPolicy rejectionPolicy() {
        return this.rejectionPolicy;
    }

    private static Consumer<Throwable> logFailure() {
        return throwable -> LOGGER.log(System.Logger.Level.ERROR, "A compute task submitted through ComputeExecutor failed", throwable);
    }

    /**
     * What an executor does when the engine refuses a submission.
     */
    public enum RejectionPolicy {

        /**
         * Run the command on the calling thread. This is the default: no work is lost, and the
         * caller already accepted the cost of running it synchronously. Exceptions thrown by the
         * command propagate to the caller, exactly like {@code ThreadPoolExecutor.CallerRunsPolicy}.
         */
        CALLER_RUNS,

        /**
         * Discard the command. Use this when running on the calling thread would violate the
         * caller's threading assumptions, for example when the caller is a region thread and the
         * work is only an optimisation.
         */
        DROP
    }
}
