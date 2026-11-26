package kz.bejiihiu.tuff.threads.bootstrap;

import java.util.Objects;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.jspecify.annotations.Nullable;

/**
 * What the bootstrap currently knows about the thread layer, as one immutable value.
 *
 * <p>The command is the only consumer. It is a value and not a live facade so the report can be built
 * and tested without a running server, and so a command run during shutdown reads one consistent moment
 * instead of half a snapshot.
 *
 * <p>{@code settings} and {@code snapshot} are absent when the runtime does not exist (disabled, not
 * started yet, failed). {@code unregisteredThreads} follows the module-wide convention that {@code -1}
 * means "we did not look": a disabled metrics section is not the same statement as "zero unknown
 * threads".
 *
 * @param state               lifecycle state of the thread layer
 * @param settings            effective settings, or {@code null} when there is no runtime
 * @param snapshot            latest resource snapshot, or {@code null} when there is no runtime
 * @param unregisteredThreads threads no pool accounts for, or {@code -1} when not measured
 * @param metricsEnabled      whether the runtime samples pool and thread state at all
 */
public record ThreadsStatus(
    State state,
    @Nullable ThreadManagementSettings settings,
    @Nullable ResourceSnapshot snapshot,
    int unregisteredThreads,
    boolean metricsEnabled
) {

    /**
     * Validates the status.
     *
     * @throws NullPointerException     if {@code state} is {@code null}
     * @throws IllegalArgumentException if the count is below {@code -1}
     */
    public ThreadsStatus {
        Objects.requireNonNull(state, "state");
        if (unregisteredThreads < -1) {
            throw new IllegalArgumentException(
                "unregisteredThreads must be -1 or non negative, got " + unregisteredThreads
            );
        }
    }

    /**
     * Returns the status of a process that has not reached the startup hook yet.
     *
     * @return the not-initialised status
     */
    public static ThreadsStatus notInitialised() {
        return new ThreadsStatus(State.NOT_INITIALISED, null, null, -1, false);
    }

    /**
     * Returns the status after the runtime was created and started.
     *
     * @param settings the settings the runtime was built with
     * @return the running status without a snapshot yet
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    public static ThreadsStatus running(final ThreadManagementSettings settings) {
        Objects.requireNonNull(settings, "settings");
        return new ThreadsStatus(
            State.RUNNING,
            settings,
            null,
            -1,
            settings.metrics().enabled() && settings.lifecycle().monitorThreads()
        );
    }

    /**
     * Returns the status of a configuration that switched the layer off.
     *
     * @param settings the settings that disabled it
     * @return the disabled status
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    public static ThreadsStatus disabled(final ThreadManagementSettings settings) {
        return new ThreadsStatus(State.DISABLED, Objects.requireNonNull(settings, "settings"), null, -1, false);
    }

    /**
     * Returns the status of a startup that failed.
     *
     * @return the failed status
     */
    public static ThreadsStatus failed() {
        return new ThreadsStatus(State.FAILED, null, null, -1, false);
    }

    /**
     * Returns a copy that carries a measured snapshot.
     *
     * @param snapshot            the snapshot to publish
     * @param unregisteredThreads the unaccounted thread count, or {@code -1} when metrics are disabled
     * @return the copy
     * @throws NullPointerException if {@code snapshot} is {@code null}
     */
    public ThreadsStatus withSnapshot(final ResourceSnapshot snapshot, final int unregisteredThreads) {
        return new ThreadsStatus(
            this.state,
            this.settings,
            Objects.requireNonNull(snapshot, "snapshot"),
            unregisteredThreads,
            this.metricsEnabled
        );
    }

    /**
     * Checks whether the runtime exists and is running.
     *
     * @return {@code true} for the running state
     */
    public boolean isRunning() {
        return this.state == State.RUNNING;
    }

    /**
     * Lifecycle state of the thread layer as the bootstrap sees it.
     */
    public enum State {

        /**
         * The startup hook has not run yet.
         */
        NOT_INITIALISED,

        /**
         * {@code thread-management.enabled: false}; no runtime exists and no thread is started.
         */
        DISABLED,

        /**
         * The runtime was created, started and every observe-only pool is registered.
         */
        RUNNING,

        /**
         * The runtime could not be built or started; the server runs without the thread layer.
         */
        FAILED
    }
}
