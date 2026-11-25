package kz.bejiihiu.tuff.compute.internal.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.jspecify.annotations.Nullable;

/**
 * Helpers for dealing with task failures.
 *
 * <p>The scheduler catches {@link Throwable} from task bodies, records them, and hands them to the
 * caller's future. These helpers make that path uniform so no component invents its own unwrapping
 * or sneaky throw.
 */
public final class Throwables {

    private Throwables() {
    }

    /**
     * Strips the wrapper exceptions the JDK adds around async failures.
     *
     * <p>{@link CompletionException} and {@link ExecutionException} are unwrapped recursively, so the
     * caller sees the throwable the task actually threw. Any other throwable is returned as is.
     *
     * @param throwable failure to unwrap
     * @return the innermost cause, or {@code throwable} itself if it is not a wrapper
     */
    public static Throwable unwrap(final Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * Rethrows any throwable without wrapping it in a checked exception declaration.
     *
     * <p>Used where a checked exception cannot be declared, for example inside a
     * {@link java.util.function.Supplier}. It is not a general purpose escape hatch; callers that can
     * declare the exception should do that instead.
     *
     * @param throwable throwable to rethrow
     * @return never returns; declared as {@link RuntimeException} so call sites can write
     *     {@code throw Throwables.rethrow(t)}
     * @throws AssertionError never thrown in practice, it only satisfies the compiler after the
     *     unreachable sneaky throw
     */
    public static RuntimeException rethrow(final Throwable throwable) {
        Throwables.<RuntimeException>throwUnchecked(throwable);
        throw new AssertionError("unreachable");
    }

    /**
     * Returns a short single line description of a throwable for logs and metrics.
     *
     * @param throwable failure to describe
     * @return {@code class simple name: message}, with the message omitted when it is empty
     */
    public static String describe(final @Nullable Throwable throwable) {
        if (throwable == null) {
            return "none";
        }
        final String message = throwable.getMessage();
        final String type = throwable.getClass().getSimpleName();
        return message == null || message.isBlank() ? type : type + ": " + message;
    }

    @SuppressWarnings("unchecked")
    private static <X extends Throwable> void throwUnchecked(final Throwable throwable) throws X {
        throw (X) throwable;
    }
}
