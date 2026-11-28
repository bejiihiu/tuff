package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.common.util.TimeUtil;
import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * A tick that reproduces the region handle's ticking mark, for the mid-tick permit tests.
 *
 * <p>{@link FakeTick} models a body but nothing else, so it cannot show the race the scheduler has to
 * avoid: a tick whose region is still marked ticking by {@code runTasks} while a second tick arrives
 * with an earlier deadline. This class marks itself for the whole pass, refuses to run a body while the
 * mark is held (exactly like the real handle throwing when the region is not acquirable), and lets the
 * test decide when the pass ends. A refused body is counted instead of silently throwing into the fund,
 * so an assertion can name the defect.
 */
final class MarkingTick extends SchedulableTick {

    private static final long PASS_WAIT_SECONDS = 20L;

    private final String label;
    private final AtomicBoolean marked = new AtomicBoolean();
    private final AtomicBoolean insidePass = new AtomicBoolean();
    private final AtomicInteger bodies = new AtomicInteger();
    private final AtomicInteger passes = new AtomicInteger();
    private final AtomicInteger refusedBodies = new AtomicInteger();
    private final CountDownLatch passEntered = new CountDownLatch(1);
    private final CountDownLatch passRelease = new CountDownLatch(1);
    private final CountDownLatch bodyEntered = new CountDownLatch(1);

    private volatile boolean tasks = true;
    private volatile long deadline;
    private volatile long nextDeadline = TimeUtil.DEADLINE_NOT_SET;

    MarkingTick(final String label, final long deadline) {
        this.label = label;
        this.deadline = deadline;
        this.setScheduledStart(deadline);
    }

    MarkingTick withoutTasks() {
        this.tasks = false;
        return this;
    }

    MarkingTick thenRescheduleAt(final long next) {
        this.nextDeadline = next;
        return this;
    }

    long deadline() {
        return this.deadline;
    }

    int bodies() {
        return this.bodies.get();
    }

    int passes() {
        return this.passes.get();
    }

    int refusedBodies() {
        return this.refusedBodies.get();
    }

    boolean isMarked() {
        return this.marked.get();
    }

    boolean isInsidePass() {
        return this.insidePass.get();
    }

    boolean awaitPassEntered(final long millis) throws InterruptedException {
        return this.passEntered.await(millis, TimeUnit.MILLISECONDS);
    }

    boolean awaitBody(final long millis) throws InterruptedException {
        return this.bodyEntered.await(millis, TimeUnit.MILLISECONDS);
    }

    void releasePass() {
        this.passRelease.countDown();
    }

    @Override
    public boolean runTick() {
        if (this.marked.get()) {
            // the real handle throws here; counting it lets the test prove nothing ever got this far
            this.refusedBodies.incrementAndGet();
            throw new IllegalStateException("Scheduled region should be acquirable");
        }
        this.bodies.incrementAndGet();
        this.bodyEntered.countDown();
        if (this.nextDeadline != TimeUtil.DEADLINE_NOT_SET) {
            this.deadline = this.nextDeadline;
            this.setScheduledStart(this.nextDeadline);
            return true;
        }
        return false;
    }

    @Override
    public boolean hasTasks() {
        return this.tasks;
    }

    @Override
    public boolean runTasks(final BooleanSupplier canContinue) {
        this.passes.incrementAndGet();
        if (!this.marked.compareAndSet(false, true)) {
            throw new IllegalStateException("region is already ticking");
        }
        this.insidePass.set(true);
        try {
            this.passEntered.countDown();
            // the test decides when the region stops being marked; ignoring canContinue is the point,
            // because that is the window in which a replacement used to arrive
            if (!this.passRelease.await(PASS_WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the pass was never released");
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } finally {
            this.insidePass.set(false);
            this.marked.set(false);
        }
        return true;
    }

    @Override
    public String toString() {
        return "MarkingTick{" + this.label + '}';
    }
}
