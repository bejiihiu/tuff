package kz.bejiihiu.tuff.threads.kairos;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * A monotonic clock a test can move, so a stale tick and the warning quiet window can be reached
 * without sleeping for them.
 */
final class MutableNanoClock implements LongSupplier {

    private final AtomicLong now;

    MutableNanoClock(final long start) {
        this.now = new AtomicLong(start);
    }

    @Override
    public long getAsLong() {
        return this.now.get();
    }

    void advance(final long nanos) {
        this.now.addAndGet(nanos);
    }
}
