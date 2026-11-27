package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
 * The Kairos fund recording.
 *
 * <p>Only the fund event belongs to this scheduler: the per-tick {@code tuff.scheduler.Tick} event is
 * written by the region handle for every scheduler, so the fund must not write a second row for the
 * same body. A real recording stream also proves the switch is what arms the code.
 */
@Timeout(120)
class KairosJfrTest {

    private static final long PERIOD = TimeUnit.MILLISECONDS.toNanos(50L);

    private final RecordingThreadFactory threads = new RecordingThreadFactory();
    private @Nullable KairosScheduler scheduler;

    @AfterEach
    void stopEverything() {
        if (this.scheduler != null) {
            this.scheduler.halt();
            assertTrue(this.scheduler.join(10_000L), "halt must stop every thread of the fund");
        }
        KairosGate.disarm();
    }

    @Test
    void everySupervisorPassIsRecordedWithTheFundState() throws Exception {
        final List<RecordedEvent> passes = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Fund", passes::add);
            stream.startAsync();
            Thread.sleep(100L);

            KairosGate.arm(true, new KairosTuning(true, 2, 5_000, 50, 20L));
            KairosGate.kernel(new KairosKernel(1, 50, 0, 3, 0, true));
            final KairosScheduler elastic = this.scheduler =
                new KairosScheduler(1, this.threads, tick -> ((CatchUpTick) tick).scheduledStart());
            elastic.start();

            assertTrue(awaitEvent(passes, 1, 5_000L), "a supervisor pass must be recorded");
            final RecordedEvent event = passes.getFirst();
            assertEquals(1, event.getInt("minThreads"));
            assertEquals(2, event.getInt("maxThreads"));
            assertTrue(event.getInt("liveThreads") >= 1);
            assertTrue(event.getInt("targetThreads") >= 1);
            assertTrue(event.getLong("spawned") >= 0L);
            assertTrue(event.getLong("retired") >= 0L);
        }
    }

    @Test
    void theFundDoesNotEmitItsOwnTickEvent() throws Exception {
        final List<RecordedEvent> ticks = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Tick", ticks::add);
            stream.startAsync();
            Thread.sleep(100L);

            final KairosScheduler fixed = this.fund(new KairosKernel(1, 50, 0, 3, 0, true));
            final CatchUpTick tick = new CatchUpTick("recorded", PERIOD, 4L, System::nanoTime, null, 0L).stopAfter(1);
            fixed.schedule(tick);
            fixed.start();
            assertTrue(tick.awaitBodies(1, 5_000L), "the tick must run");
            Thread.sleep(300L);

            assertTrue(ticks.isEmpty(), "the region handle owns the tick event, the fund must not duplicate it: " + ticks);
        }
    }

    private KairosScheduler fund(final KairosKernel kernel) {
        KairosGate.arm(true, new KairosTuning(false, -1, 5_000, 10, 20L));
        KairosGate.kernel(kernel);
        return this.scheduler = new KairosScheduler(
            1,
            this.threads,
            tick -> ((CatchUpTick) tick).scheduledStart(),
            new KairosTickControl(
                tick -> ((CatchUpTick) tick).missedPeriods(),
                (tick, periods) -> ((CatchUpTick) tick).limitWriteOff(periods)
            ),
            System::nanoTime
        );
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
}
