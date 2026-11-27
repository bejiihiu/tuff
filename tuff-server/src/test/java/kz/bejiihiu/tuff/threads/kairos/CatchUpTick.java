package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * A tick that keeps its own schedule, so catch-up can be tested without a server.
 *
 * <p>It reproduces the two facts the fund relies on in Folia's region handle: the number of elapsed
 * periods between the last handled period and now, and the write-off limit that decides how many of
 * them a body accounts for. The arithmetic is copied from {@code RegionScheduleHandle.runTick}: the
 * body moves the period counter by the number it accounted for and reschedules at the later of now
 * and the next deadline, which is the clamp that keeps a slow region from starving the rest.
 *
 * <p>A test can freeze or move the clock and can ask the tick to stop or cancel itself, so body
 * counts, skipped periods and thread names are all observable.
 */
final class CatchUpTick extends SchedulableTick {

    private final String label;
    private final long period;
    private final LongSupplier clock;
    private final MutableNanoClock movable;
    private final long bodyNanos;
    private final List<String> threads = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> limits = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger executed = new AtomicInteger();
    private final AtomicInteger skipped = new AtomicInteger();

    private long lastPeriod;
    private volatile long writeOffLimit;
    private volatile int stopAfter = Integer.MAX_VALUE;
    private volatile int cancelAfter = Integer.MAX_VALUE;
    private volatile boolean cancelled;

    /**
     * Creates a tick described by a deadline and a period, for tests that do not care about the
     * write-off model.
     *
     * @param label  name used in failure messages
     * @param start  first deadline
     * @param period period of the model schedule
     * @param clock  clock shared with the scheduler
     */
    CatchUpTick(final String label, final long start, final long period, final LongSupplier clock) {
        this.label = label;
        this.period = period;
        this.clock = clock;
        this.movable = null;
        this.bodyNanos = 0L;
        this.lastPeriod = start - period;
        this.setScheduledStart(start);
    }

    /**
     * Creates a tick that is {@code periodsLate} whole periods behind when the scheduler starts.
     *
     * @param label       name used in failure messages
     * @param period      period of the model schedule, in nanoseconds
     * @param periodsLate whole periods the tick is already late by
     * @param clock       clock shared with the scheduler
     * @param movable     the same clock when a test wants body time to advance it, or {@code null}
     * @param bodyNanos   how much a body advances the clock, ignored when {@code movable} is null
     */
    CatchUpTick(
        final String label,
        final long period,
        final long periodsLate,
        final LongSupplier clock,
        final MutableNanoClock movable,
        final long bodyNanos
    ) {
        this.label = label;
        this.period = period;
        this.clock = clock;
        this.movable = movable;
        this.bodyNanos = bodyNanos;
        final long firstStart = clock.getAsLong() - periodsLate * period;
        this.lastPeriod = firstStart - period;
        this.setScheduledStart(firstStart);
    }

    CatchUpTick stopAfter(final int bodies) {
        this.stopAfter = bodies;
        return this;
    }

    CatchUpTick cancelAfter(final int bodies) {
        this.cancelAfter = bodies;
        return this;
    }

    CatchUpTick cancel() {
        this.cancelled = true;
        return this;
    }

    /**
     * Returns how many whole periods this handle is behind, exactly as the region handle answers.
     *
     * @return the elapsed periods behind the deadline, never negative
     */
    long missedPeriods() {
        if (this.cancelled) {
            return 0L;
        }
        return Math.max(0L, (this.clock.getAsLong() - this.lastPeriod) / this.period);
    }

    /**
     * Applies the write-off limit the fund asked for.
     *
     * @param periods periods the next body may account for, or zero for the whole backlog
     */
    void limitWriteOff(final long periods) {
        this.limits.add(periods);
        this.writeOffLimit = periods;
    }

    long scheduledStart() {
        return this.getScheduledStart();
    }

    int calls() {
        return this.calls.get();
    }

    int executed() {
        return this.executed.get();
    }

    int skippedPeriods() {
        return this.skipped.get();
    }

    int limitCalls() {
        return this.limits.size();
    }

    long lastLimit() {
        final List<Long> seen = List.copyOf(this.limits);
        return seen.isEmpty() ? Long.MIN_VALUE : seen.getLast();
    }

    List<String> threadNames() {
        return List.copyOf(this.threads);
    }

    @Override
    public boolean runTick() {
        final int call = this.calls.incrementAndGet();
        if (this.cancelled) {
            return false;
        }
        this.threads.add(Thread.currentThread().getName());
        this.executed.incrementAndGet();

        final long now = this.clock.getAsLong();
        final long ahead = Math.max(0L, (now - this.lastPeriod) / this.period);
        final long limit = this.writeOffLimit;
        final long accounted = Math.max(1L, limit <= 0L ? ahead : Math.min(ahead, limit));
        this.skipped.addAndGet((int) (accounted - 1L));

        // the clamp of the region handle: a body never schedules itself into the past
        this.lastPeriod += accounted * this.period;
        this.setScheduledStart(Math.max(now, this.lastPeriod + this.period));

        if (this.movable != null && this.bodyNanos > 0L) {
            this.movable.advance(this.bodyNanos);
        }
        if (call >= this.cancelAfter) {
            this.cancelled = true;
            return false;
        }
        return call < this.stopAfter;
    }

    @Override
    public boolean hasTasks() {
        return false;
    }

    @Override
    public boolean runTasks(final BooleanSupplier canContinue) {
        return false;
    }

    @Override
    public String toString() {
        return "CatchUpTick{" + this.label + '}';
    }

    /**
     * Waits until the tick ran the requested number of bodies.
     *
     * @param expected body count to wait for
     * @param millis   budget of the wait
     * @return {@code true} when the count was reached
     * @throws InterruptedException if the waiting thread is interrupted
     */
    boolean awaitBodies(final int expected, final long millis) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (this.calls.get() < expected) {
            if (System.nanoTime() - deadline >= 0L) {
                return false;
            }
            Thread.sleep(1L);
        }
        return true;
    }
}
