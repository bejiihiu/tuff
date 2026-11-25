package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Thread safe clock for adaptive controller tests: nothing sleeps, time only moves when a test says
 * so.
 */
final class TestComputeClock implements ComputeClock {

    private final AtomicLong nanos = new AtomicLong();
    private final Instant origin = Instant.parse("2026-01-01T00:00:00Z");

    @Override
    public long nanoTime() {
        return this.nanos.get();
    }

    @Override
    public Instant instant() {
        return this.origin.plusNanos(this.nanos.get());
    }

    /**
     * Moves the clock forward.
     *
     * @param amount how far, must not be negative
     */
    void advance(final Duration amount) {
        this.nanos.addAndGet(amount.toNanos());
    }
}
