package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.Objects;
import java.util.Optional;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import org.jspecify.annotations.Nullable;

/**
 * What the bootstrap currently knows about the compute engine.
 *
 * <p>The four states are deliberately more than "present or absent": an administrator has to be able
 * to tell a deliberate {@code async-compute.enabled: false} from a failure, and the command prints a
 * different sentence for each. A running state always carries the scheduler; every other state never
 * does, which the constructor enforces.
 *
 * @param state     lifecycle state of the engine
 * @param scheduler the running scheduler when the state is {@link State#RUNNING}
 */
public record ComputeStatus(State state, @Nullable AsyncComputeScheduler scheduler) {

    /**
     * Validates the state and the scheduler reference.
     *
     * @param state     lifecycle state of the engine
     * @param scheduler the running scheduler when the state is {@link State#RUNNING}
     * @throws NullPointerException     if {@code state} is {@code null}
     * @throws IllegalArgumentException if the state and the scheduler do not belong together
     */
    public ComputeStatus {
        Objects.requireNonNull(state, "state");
        if (state == State.RUNNING && scheduler == null) {
            throw new IllegalArgumentException("a running compute engine needs a scheduler");
        }
        if (state != State.RUNNING && scheduler != null) {
            throw new IllegalArgumentException("only a running compute engine may carry a scheduler, got " + state);
        }
    }

    /**
     * Returns the state used before the bootstrap has run, for example very early in startup.
     *
     * @return a not initialised status
     */
    public static ComputeStatus notInitialised() {
        return new ComputeStatus(State.NOT_INITIALISED, null);
    }

    /**
     * Returns the state of an engine that the configuration switched off.
     *
     * @return a disabled status
     */
    public static ComputeStatus disabled() {
        return new ComputeStatus(State.DISABLED, null);
    }

    /**
     * Returns the state of an engine that could not be started.
     *
     * @return a failed status
     */
    public static ComputeStatus failed() {
        return new ComputeStatus(State.FAILED, null);
    }

    /**
     * Returns the state of a running engine.
     *
     * @param scheduler the installed scheduler
     * @return a running status
     * @throws NullPointerException if {@code scheduler} is {@code null}
     */
    public static ComputeStatus running(final AsyncComputeScheduler scheduler) {
        return new ComputeStatus(State.RUNNING, Objects.requireNonNull(scheduler, "scheduler"));
    }

    /**
     * Checks whether the engine is installed and accepting work.
     *
     * @return {@code true} if the state is {@link State#RUNNING}
     */
    public boolean isRunning() {
        return this.state == State.RUNNING;
    }

    /**
     * Returns the scheduler when the engine is running.
     *
     * @return the scheduler, or an empty optional for every other state
     */
    public Optional<AsyncComputeScheduler> engine() {
        return Optional.ofNullable(this.scheduler);
    }

    /**
     * Lifecycle state of the engine as a whole.
     */
    public enum State {

        /**
         * The bootstrap has not run yet, so nothing is known and nothing is installed.
         */
        NOT_INITIALISED,

        /**
         * {@code async-compute.enabled} is {@code false}: no pool exists and every submission was
         * refused by not having an engine at all.
         */
        DISABLED,

        /**
         * The engine is running and installed for plugins.
         */
        RUNNING,

        /**
         * The engine could not be created or started. The server runs without it and the cause is in
         * the log.
         */
        FAILED
    }
}
