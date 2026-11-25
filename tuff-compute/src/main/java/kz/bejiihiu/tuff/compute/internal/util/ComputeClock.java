package kz.bejiihiu.tuff.compute.internal.util;

import java.time.Instant;

/**
 * Time source used by every scheduler component.
 *
 * <p>All durations inside the scheduler are measured with {@link #nanoTime()}, because it is
 * monotonic and immune to wall clock jumps. {@link #instant()} exists only for timestamps that are
 * shown to humans, for example {@link kz.bejiihiu.tuff.compute.ComputeTaskHandle#submittedAt()}.
 *
 * <p>Production code always uses {@link #system()}. Tests may supply a fake implementation to drive
 * aging, queue expiry and adaptive windows deterministically, which is the reason this indirection
 * exists at all.
 */
public interface ComputeClock {

    /**
     * Returns the current value of the monotonic clock.
     *
     * <p>Only differences between two readings are meaningful; the origin is arbitrary and differs
     * between clock instances.
     *
     * @return monotonic nanoseconds
     */
    long nanoTime();

    /**
     * Returns the current wall clock instant.
     *
     * <p>Used for human facing timestamps only, never for measuring durations.
     *
     * @return the current instant
     */
    Instant instant();

    /**
     * Returns how many nanoseconds passed since the given monotonic reading.
     *
     * @param startNanos reading taken earlier with {@link #nanoTime()}
     * @return elapsed nanoseconds, never negative in practice because the clock is monotonic
     */
    default long elapsedNanos(final long startNanos) {
        return this.nanoTime() - startNanos;
    }

    /**
     * Returns the clock backed by {@link System#nanoTime()} and {@link Instant#now()}.
     *
     * @return the system clock
     */
    static ComputeClock system() {
        return SystemClock.INSTANCE;
    }

    /**
     * Clock implementation backed by the JVM.
     */
    final class SystemClock implements ComputeClock {

        private static final SystemClock INSTANCE = new SystemClock();

        private SystemClock() {
        }

        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public Instant instant() {
            return Instant.now();
        }
    }
}
