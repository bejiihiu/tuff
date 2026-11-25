package kz.bejiihiu.tuff.compute;

/**
 * Thrown when a submission is refused because the scheduler is not accepting work.
 *
 * <p>This happens before the scheduler was started and after {@link AsyncComputeScheduler#shutdown()}
 * was called. It is deliberately separate from {@link ComputeRejectedException}: refusal because of
 * limits is recoverable, refusal because of the lifecycle is not, and callers usually treat the two
 * differently.
 */
public final class ComputeShutdownException extends ComputeException {

    /**
     * Creates the exception.
     *
     * @param message detail message, usually naming the current scheduler state
     */
    public ComputeShutdownException(final String message) {
        super(message);
    }
}
