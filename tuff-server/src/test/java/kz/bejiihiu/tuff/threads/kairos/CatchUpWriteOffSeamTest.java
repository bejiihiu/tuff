package kz.bejiihiu.tuff.threads.kairos;

import io.papermc.paper.threadedregions.TickRegionScheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The catch-up write-off seam on the real region handle, not on a copy of its arithmetic.
 *
 * <p>{@code limitWriteOff} is set by the scheduler just before a body and read by {@code runTick}. The
 * limit must belong to exactly one body: a handle whose value survived would write off a single period
 * on every later body, so a region that was once late would silently lose tick rate with the wall clock
 * standing still. The old suite compared a handwritten copy of the arithmetic with itself, which cannot
 * see this.
 *
 * <p>The body is driven by Folia's own EDF scheduler because {@code runTick} requires a tick thread;
 * that is also the path the real server uses.
 */
@Timeout(120)
class CatchUpWriteOffSeamTest {

    private @Nullable TickRegionScheduler scheduler;

    @AfterEach
    void stopEverything() {
        final TickRegionScheduler current = this.scheduler;
        this.scheduler = null;
        if (current != null) {
            assertTrue(current.halt(true, 10_000L), "the scheduler must stop");
        }
    }

    @Test
    void theLimitIsConsumedByExactlyOneBody() throws InterruptedException {
        // several periods behind, so a body with the limit reports one and one without reports more
        final long firstStart = System.nanoTime() - 4L * TickRegionScheduler.TIME_BETWEEN_TICKS;
        final WriteOffHandle handle = new WriteOffHandle(firstStart);
        final TickRegionScheduler current = new TickRegionScheduler(TickRegionScheduler.SchedulerType.EDF, 1);
        this.scheduler = current;
        current.start();
        current.scheduleRegion(handle);

        assertTrue(handle.awaitBodies(5_000L), "two bodies must run: " + handle.bodies());
        handle.markNonSchedulable();

        final List<Long> bodies = handle.bodies();
        assertEquals(1L, bodies.get(0), "a limited body accounts for one period");
        assertTrue(bodies.get(1) > 1L,
            "without a new limit the body must write off the whole backlog, saw " + bodies);
        assertEquals(0L, handle.pendingWriteOffLimit(), "the body consumed the limit");
    }

    /**
     * A handle with no region that records how many periods each body accounted for.
     */
    private static final class WriteOffHandle extends TickRegionScheduler.RegionScheduleHandle {

        private final List<Long> bodies = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch twoBodies = new CountDownLatch(2);

        private WriteOffHandle(final long firstStart) {
            super(null, firstStart);
            // the limit a catch-up burst would set: it must be consumed by the first body only
            this.limitWriteOff(1L);
        }

        private boolean awaitBodies(final long millis) throws InterruptedException {
            return this.twoBodies.await(millis, TimeUnit.MILLISECONDS);
        }

        private List<Long> bodies() {
            synchronized (this.bodies) {
                return List.copyOf(this.bodies);
            }
        }

        @Override
        protected boolean tryMarkTicking() {
            return true;
        }

        @Override
        protected boolean markNotTicking() {
            return true;
        }

        @Override
        protected void tickRegion(final long tickCount, final long startTime, final long scheduledEnd) {
            this.bodies.add(tickCount);
            this.twoBodies.countDown();
        }

        @Override
        protected void runRegionTasks(final BooleanSupplier canContinue) {
            // no intermediate work
        }

        @Override
        protected boolean hasIntermediateTasks() {
            return false;
        }
    }
}
