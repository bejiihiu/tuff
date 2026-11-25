package kz.bejiihiu.tuff.compute.internal.util;

import java.util.Objects;

/**
 * Argument and state checks used across the scheduler.
 *
 * <p>Every method fails fast and includes the offending value in the message, because these
 * exceptions end up in server logs where "value must be positive" without a value helps nobody.
 */
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Checks that a reference is not {@code null}.
     *
     * @param value value to check
     * @param name  name of the value, used in the message
     * @param <T>   type of the value
     * @return the value, unchanged
     * @throws NullPointerException if {@code value} is {@code null}
     */
    public static <T> T requireNonNull(final T value, final String name) {
        return Objects.requireNonNull(value, name);
    }

    /**
     * Checks that a string is neither {@code null} nor blank.
     *
     * @param value string to check
     * @param name  name of the value, used in the message
     * @return the string, unchanged
     * @throws NullPointerException     if {@code value} is {@code null}
     * @throws IllegalArgumentException if {@code value} is blank
     */
    public static String requireNonBlank(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    /**
     * Checks that a value is strictly positive.
     *
     * @param value value to check
     * @param name  name of the value, used in the message
     * @return the value, unchanged
     * @throws IllegalArgumentException if {@code value} is not positive
     */
    public static int checkPositive(final int value, final String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least 1, got " + value);
        }
        return value;
    }

    /**
     * Checks that a value is at least the given minimum.
     *
     * @param value   value to check
     * @param minimum inclusive minimum
     * @param name    name of the value, used in the message
     * @return the value, unchanged
     * @throws IllegalArgumentException if {@code value} is below {@code minimum}
     */
    public static int checkAtLeast(final int value, final int minimum, final String name) {
        if (value < minimum) {
            throw new IllegalArgumentException(name + " must be at least " + minimum + ", got " + value);
        }
        return value;
    }

    /**
     * Checks that a value is not negative.
     *
     * @param value value to check
     * @param name  name of the value, used in the message
     * @return the value, unchanged
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static long checkNonNegative(final long value, final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must not be negative, got " + value);
        }
        return value;
    }

    /**
     * Checks a condition that depends on the caller's arguments.
     *
     * @param condition condition that must hold
     * @param message   message used when it does not
     * @throws IllegalArgumentException if {@code condition} is {@code false}
     */
    public static void checkArgument(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks a condition that depends on the scheduler's own state.
     *
     * @param condition condition that must hold
     * @param message   message used when it does not
     * @throws IllegalStateException if {@code condition} is {@code false}
     */
    public static void checkState(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
