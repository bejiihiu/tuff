package kz.bejiihiu.tuff.threads;

/**
 * Base class for failures of the unified thread manager.
 *
 * <p>All exceptions thrown by this API are unchecked: the thread layer is called from bootstrap code,
 * command handlers and scheduling paths where a checked exception would only be wrapped. A caller that
 * can recover checks the specific subtype; everything else is a programming or configuration error and
 * is allowed to reach the server's uncaught handler.
 */
public class ThreadManagementException extends RuntimeException {

    /**
     * Creates an exception with a message.
     *
     * @param message detail message
     */
    public ThreadManagementException(final String message) {
        super(message);
    }

    /**
     * Creates an exception with a message and a cause.
     *
     * @param message detail message
     * @param cause   underlying failure
     */
    public ThreadManagementException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
