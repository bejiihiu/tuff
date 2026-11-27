package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.common.util.TimeUtil;
import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * A tick that records when it runs, so the scheduler can be tested without a server.
 *
 * <p>The deadline is kept in a field as well as in the tick, because the scheduler reads it through
 * the accessor the facade supplies; a real handle writes both in {@code runTick}.
 */
final class FakeTick extends SchedulableTick {

    private final String label;
    private final List<String> order;
    private final CountDownLatch done;
    private final AtomicInteger runs = new AtomicInteger();
    private final CountDownLatch entered = new CountDownLatch(1);

    private volatile long deadline;
    private volatile long nextDeadline = TimeUtil.DEADLINE_NOT_SET;
    private volatile long runNanos;

    FakeTick(final String label, final long deadline, final List<String> order, final CountDownLatch done) {
        this.label = label;
        this.deadline = deadline;
        this.order = order;
        this.done = done;
        this.setScheduledStart(deadline);
    }

    FakeTick runFor(final long nanos) {
        this.runNanos = nanos;
        return this;
    }

    FakeTick thenRescheduleAt(final long next) {
        this.nextDeadline = next;
        return this;
    }

    long deadline() {
        return this.deadline;
    }

    int runs() {
        return this.runs.get();
    }

    CountDownLatch entered() {
        return this.entered;
    }

    @Override
    public boolean runTick() {
        this.order.add(this.label);
        this.runs.incrementAndGet();
        this.entered.countDown();
        if (this.runNanos > 0L) {
            try {
                // sleep, not park: halt() unparks a parked runner and would fake a short tick
                TimeUnit.NANOSECONDS.sleep(this.runNanos);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }
        if (this.nextDeadline != TimeUtil.DEADLINE_NOT_SET) {
            this.deadline = this.nextDeadline;
            this.setScheduledStart(this.nextDeadline);
            this.done.countDown();
            return true;
        }
        this.done.countDown();
        return false;
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
        return "FakeTick{" + this.label + "}";
    }
}
