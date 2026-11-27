package kz.bejiihiu.tuff.threads.kairos;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * A tick that models the region handle's intermediate task path, for the idle work tests.
 *
 * <p>{@link FakeTick} already models the body and the reschedule; this subclass adds what the mid-tick
 * pass actually calls: {@code hasTasks} and {@code runTasks}. A test chooses whether a pass drains one
 * task or loops until the supplied {@code canContinue} turns false, and whether the pass deliberately
 * ignores that answer so the self-protection can be observed. The counters make "how many passes" and
 * "how long the pass took" exact questions.
 */
final class IdleTick extends FakeTick {

    private final AtomicInteger hasTaskCalls = new AtomicInteger();
    private final AtomicInteger passCalls = new AtomicInteger();
    private final AtomicInteger units = new AtomicInteger();

    private volatile boolean tasks = true;
    private volatile boolean drain = true;
    private volatile long overrunNanos;
    private volatile long passStartNanos;
    private volatile long passEndNanos;
    private volatile boolean cancelled;

    IdleTick(final String label, final long deadline, final List<String> order, final CountDownLatch done) {
        super(label, deadline, order, done);
    }

    IdleTick withNoTasks() {
        this.tasks = false;
        return this;
    }

    IdleTick withOneTaskPerPass() {
        this.drain = false;
        return this;
    }

    /**
     * Makes the pass ignore its cancellation flag and burn the given wall clock, which is what a badly
     * behaved unit looks like from the outside.
     *
     * @param nanos time to burn inside the pass
     * @return this tick
     */
    IdleTick ignoringTheSliceFor(final long nanos) {
        this.overrunNanos = nanos;
        return this;
    }

    IdleTick cancel() {
        this.cancelled = true;
        return this;
    }

    boolean isCancelled() {
        return this.cancelled;
    }

    int hasTaskCalls() {
        return this.hasTaskCalls.get();
    }

    int passCalls() {
        return this.passCalls.get();
    }

    int taskUnits() {
        return this.units.get();
    }

    long passNanos() {
        return this.passEndNanos - this.passStartNanos;
    }

    @Override
    public boolean hasTasks() {
        this.hasTaskCalls.incrementAndGet();
        return this.tasks;
    }

    @Override
    public boolean runTasks(final BooleanSupplier canContinue) {
        this.passCalls.incrementAndGet();
        this.passStartNanos = System.nanoTime();
        try {
            final long overrun = this.overrunNanos;
            if (overrun > 0L) {
                final long until = System.nanoTime() + overrun;
                while (System.nanoTime() - until < 0L) {
                    Thread.onSpinWait();
                }
                this.units.incrementAndGet();
                return true;
            }
            while (true) {
                this.units.incrementAndGet();
                if (!this.drain || !canContinue.getAsBoolean()) {
                    return true;
                }
            }
        } finally {
            this.passEndNanos = System.nanoTime();
        }
    }
}
