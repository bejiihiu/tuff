package kz.bejiihiu.tuff.compute;

import org.jspecify.annotations.Nullable;

/**
 * Base type of every failure the scheduler reports by throwing.
 *
 * <p>It is unchecked on purpose: submissions happen from region threads and from gameplay code,
 * where a checked exception would only be wrapped again. Failures inside a task body are not
 * reported through this type; they are delivered to the caller's future.
 */
public class ComputeException extends RuntimeException {

    /**
     * Creates an exception with a message.
     *
     * @param message detail message
     */
    public ComputeException(final String message) {
        super(message);
    }

    /**
     * Creates an exception with a message and a cause.
     *
     * @param message detail message
     * @param cause   underlying failure, may be {@code null}
     */
    public ComputeException(final String message, final @Nullable Throwable cause) {
        super(message, cause);
    }
}
