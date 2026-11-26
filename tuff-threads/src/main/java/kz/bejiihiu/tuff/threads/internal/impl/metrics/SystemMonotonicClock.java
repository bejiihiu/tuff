package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.time.Instant;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;

/**
 * {@link MonotonicClock} backed by the JVM clocks.
 *
 * <p>{@link System#nanoTime()} is the only timeline that can never jump backwards, which is what lease
 * deadlines and sampling windows need. {@link Instant#now()} is read separately and used only for the
 * timestamp an operator sees, because a wall clock adjustment may move it in either direction.
 */
public final class SystemMonotonicClock implements MonotonicClock {

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public Instant instant() {
        return Instant.now();
    }
}
