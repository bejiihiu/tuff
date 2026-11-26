package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;

/**
 * Clock that only moves when a test moves it.
 *
 * <p>{@link #instant()} is fixed: it exists for display only and no assertion depends on it.
 */
final class FakeClock implements MonotonicClock {

    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);

    @Override
    public long nanoTime() {
        return this.nanos.get();
    }

    @Override
    public Instant instant() {
        return Instant.EPOCH;
    }

    /**
     * Moves the clock forward.
     *
     * @param delta nanoseconds to add, not negative
     */
    void advance(final long delta) {
        this.nanos.addAndGet(delta);
    }
}
