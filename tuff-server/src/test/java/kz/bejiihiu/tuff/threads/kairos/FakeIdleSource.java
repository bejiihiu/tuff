package kz.bejiihiu.tuff.threads.kairos;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.threads.idle.IdleWork;
import kz.bejiihiu.tuff.threads.idle.IdleWorkSource;

/**
 * An idle-safe source a test drives by hand.
 *
 * <p>It counts polls separately from runs, because the two mean different things: a poll that returns
 * nothing is the source saying "not due", while a run is a slice that actually happened. A test can
 * make the job well behaved (returns when {@code canContinue} says so) or ignore the flag for a wall
 * clock stretch, which is how the self-protection is exercised without a real blocked thread.
 */
final class FakeIdleSource implements IdleWorkSource {

    private final AtomicInteger polls = new AtomicInteger();
    private final AtomicInteger runs = new AtomicInteger();
    private final AtomicInteger units = new AtomicInteger();
    private final AtomicLong runNanos = new AtomicLong();
    private final CountDownLatch firstRun = new CountDownLatch(1);

    private volatile boolean offer = true;
    private volatile boolean drain = true;
    private volatile long overrunNanos;

    FakeIdleSource withNothingToOffer() {
        this.offer = false;
        return this;
    }

    FakeIdleSource withOneUnitPerJob() {
        this.drain = false;
        return this;
    }

    /**
     * Makes every job ignore its cancellation flag for the given wall clock.
     *
     * @param nanos time to burn inside the job
     * @return this source
     */
    FakeIdleSource ignoringTheSliceFor(final long nanos) {
        this.overrunNanos = nanos;
        return this;
    }

    int polls() {
        return this.polls.get();
    }

    int runs() {
        return this.runs.get();
    }

    int units() {
        return this.units.get();
    }

    long lastRunNanos() {
        return this.runNanos.get();
    }

    /**
     * Waits until the first job started, which is how a test lines up a tick with a running job.
     *
     * @param millis budget of the wait
     * @return {@code true} when a job started
     * @throws InterruptedException if the waiting thread is interrupted
     */
    boolean awaitFirstRun(final long millis) throws InterruptedException {
        return this.firstRun.await(millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public IdleWork poll() {
        this.polls.incrementAndGet();
        if (!this.offer) {
            return null;
        }
        return this::runOnce;
    }

    private void runOnce(final BooleanSupplier canContinue) {
        this.firstRun.countDown();
        this.runs.incrementAndGet();
        final long start = System.nanoTime();
        try {
            final long overrun = this.overrunNanos;
            if (overrun > 0L) {
                final long until = start + overrun;
                while (System.nanoTime() - until < 0L) {
                    Thread.onSpinWait();
                }
                this.units.incrementAndGet();
                return;
            }
            while (true) {
                this.units.incrementAndGet();
                if (!this.drain || !canContinue.getAsBoolean()) {
                    return;
                }
            }
        } finally {
            this.runNanos.set(System.nanoTime() - start);
        }
    }
}
