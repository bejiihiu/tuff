package kz.bejiihiu.tuff.api.compute;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Reports that a compute result could not be delivered to its target context.
 *
 * <p>This is not a computation failure: the task itself succeeded, but the region, entity or plugin
 * that was supposed to apply the result is no longer usable. The most common case is an entity that
 * was removed before the result arrived.
 *
 * <p>The exception is passed to the failure handler of the corresponding
 * {@link ComputeHandoff} call. It is never thrown out of a scheduler worker; it is data for the
 * plugin that asked for the handoff.
 */
public final class HandoffException extends RuntimeException {

    private final Reason reason;

    /**
     * Creates an exception with a message.
     *
     * @param reason  why the result could not be delivered
     * @param message detail message for logs
     */
    public HandoffException(final Reason reason, final String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    /**
     * Creates an exception with a message and a cause.
     *
     * @param reason  why the result could not be delivered
     * @param message detail message for logs
     * @param cause   underlying failure, may be {@code null}
     */
    public HandoffException(final Reason reason, final String message, final @Nullable Throwable cause) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    /**
     * Returns why the result could not be delivered.
     *
     * @return the delivery failure reason
     */
    public Reason reason() {
        return this.reason;
    }

    /**
     * Why a compute result was not delivered.
     */
    public enum Reason {

        /**
         * The target entity was removed before the result could be applied. The entity scheduler
         * reported its retired state, so no world access happened.
         */
        ENTITY_RETIRED,

        /**
         * The owning plugin was disabled before the result could be scheduled. Bukkit refuses new
         * tasks for disabled plugins; pending tasks of a plugin that gets disabled are cancelled by
         * the server without invoking the result callback.
         */
        PLUGIN_DISABLED,

        /**
         * The target scheduler rejected the delivery for an unexpected reason, for example because
         * the server is already shutting down. The original throwable is attached as the cause.
         */
        SCHEDULE_FAILED
    }
}
