package kz.bejiihiu.tuff.threads.observe;

import io.papermc.paper.threadedregions.TickRegionScheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scheduler independent per-tick observation, proved on the real region handle.
 *
 * <p>The handle is driven by Folia's EDF scheduler, so the test exercises the same code every
 * scheduler uses: the body measures its end, the schedule advances, and only then the observer sees
 * the row. The deadline in the row is captured before that advance, which is the difference between an
 * exact lag and a reconstructed one.
 */
@Timeout(120)
class TickObservationTest {

    private @Nullable TickRegionScheduler scheduler;

    @AfterEach
    void stopEverything() {
        TickObservers.uninstall();
        final TickRegionScheduler current = this.scheduler;
        this.scheduler = null;
        if (current != null) {
            assertTrue(current.halt(true, 10_000L), "the scheduler must stop");
        }
    }

    @Test
    void anEdfBodyIsRecordedAgainstTheDeadlineItRanAgainst() throws Exception {
        final List<RecordedEvent> events = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Tick", events::add);
            stream.startAsync();
            // the recording must register the event before the first body, otherwise isEnabled() is
            // still false for that body and the test measures the race, not the behaviour
            Thread.sleep(100L);
            TickObservers.install(JfrTickObserver.INSTANCE);

            final long firstStart = System.nanoTime() - 4L * TickRegionScheduler.TIME_BETWEEN_TICKS;
            final ProbeHandle handle = new ProbeHandle(firstStart);
            final TickRegionScheduler current =
                new TickRegionScheduler(TickRegionScheduler.SchedulerType.EDF, 1);
            this.scheduler = current;
            current.start();
            current.scheduleRegion(handle);
            assertTrue(handle.awaitBody(5_000L), "the body must run");
            assertTrue(awaitEvent(events, 1, 5_000L), "the body must be recorded");

            final RecordedEvent event = events.getFirst();
            assertEquals(handle.id, event.getLong("tickId"));
            assertEquals(-1L, event.getLong("regionId"), "a handle without a region reports none");
            assertEquals("EDF", event.getString("origin"));
            assertEquals(firstStart, event.getLong("scheduledStartNanos"),
                "the event must carry the deadline the body was measured against, before the advance");
            assertTrue(handle.scheduledStart() > event.getLong("scheduledStartNanos"),
                "the handle has advanced its deadline after the body");
            assertEquals(1, events.size(), "exactly one row for one body");
            assertTrue(event.getLong("missedPeriods") >= 4L,
                "the handle is several periods behind: " + event);
            assertEquals(event.getLong("missedPeriods") - 1L, event.getLong("skipped"),
                "a stock body writes off every period it did not run");
            assertEquals(0, event.getInt("catchUpTicks"), "catch up is off on the stock path");
            assertTrue(event.getLong("executedNanos") >= 0L);
        }
    }

    @Test
    void recordingOffLeavesTheTickPathSilent() throws Exception {
        final List<RecordedEvent> events = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Tick", events::add);
            stream.startAsync();
            Thread.sleep(100L);
            TickObservers.uninstall();

            final ProbeHandle handle = new ProbeHandle(System.nanoTime() - 4L * TickRegionScheduler.TIME_BETWEEN_TICKS);
            final TickRegionScheduler current =
                new TickRegionScheduler(TickRegionScheduler.SchedulerType.EDF, 1);
            this.scheduler = current;
            current.start();
            current.scheduleRegion(handle);
            assertTrue(handle.awaitBody(5_000L), "the body must run");
            Thread.sleep(300L);

            assertTrue(events.isEmpty(), "observation is off, so the recording must stay empty: " + events);
        }
    }

    @Test
    void theSchedulerVariantIsTheRecordedOrigin() throws Exception {
        assertEquals(TickOrigin.EDF, TickRegionScheduler.SchedulerType.EDF.origin());
        assertEquals(TickOrigin.WORK_STEALING, TickRegionScheduler.SchedulerType.WORK_STEALING.origin());
        assertEquals(TickOrigin.KAIROS, TickRegionScheduler.SchedulerType.KAIROS.origin());

        final List<RecordedEvent> events = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Tick", events::add);
            stream.startAsync();
            Thread.sleep(100L);
            TickObservers.install(JfrTickObserver.INSTANCE);

            JfrTickObserver.INSTANCE.onTick(TickOrigin.KAIROS, 7L, 11L, 1_000L, 1_200L, 1_300L, 1L, 1L);
            assertTrue(awaitEvent(events, 1, 5_000L), "the direct observation must be recorded");

            final RecordedEvent event = events.getFirst();
            assertEquals("KAIROS", event.getString("origin"));
            assertEquals(7L, event.getLong("regionId"));
            assertEquals(11L, event.getLong("tickId"));
            assertEquals(1_000L, event.getLong("scheduledStartNanos"));
            assertEquals(1L, event.getLong("missedPeriods"));
            assertEquals(0L, event.getLong("skipped"));
        }
    }

    private static boolean awaitEvent(
        final List<RecordedEvent> events,
        final int expected,
        final long waitMillis
    ) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        while (events.size() < expected) {
            if (System.nanoTime() - deadline >= 0L) {
                return false;
            }
            Thread.sleep(10L);
        }
        assertNotNull(events.getFirst());
        return true;
    }

    /**
     * A handle with no region that runs one body and then stops itself.
     */
    private static final class ProbeHandle extends TickRegionScheduler.RegionScheduleHandle {

        private final CountDownLatch body = new CountDownLatch(1);

        private ProbeHandle(final long firstStart) {
            super(null, firstStart);
        }

        private boolean awaitBody(final long waitMillis) throws InterruptedException {
            return this.body.await(waitMillis, TimeUnit.MILLISECONDS);
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
            this.body.countDown();
            this.markNonSchedulable();
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
