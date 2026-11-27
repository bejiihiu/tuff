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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two recording events that turn a per-region average into a real hard miss.
 *
 * <p>A recording stream is a real JFR recording, so the test also proves the switch is what arms the
 * code: with the switch off the very same recording sees nothing, which is the promise that an
 * unrecorded server does not pay for the event.
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
    void everyExecutedTickIsRecordedWithItsDeadline() throws Exception {
        final List<RecordedEvent> ticks = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Tick", ticks::add);
            stream.startAsync();
            // the recording has to register the event before the first body, otherwise isEnabled() is
            // still false for that body and the test would measure the race, not the behaviour
            Thread.sleep(100L);

            final KairosScheduler fixed = this.fund(new KairosKernel(1, 50, 0, 3, 0, true));
            final CatchUpTick tick = new CatchUpTick("recorded", PERIOD, 4L, System::nanoTime, null, 0L).stopAfter(1);
            fixed.schedule(tick);
            fixed.start();
            assertTrue(awaitEvent(ticks, 1, 5_000L), "the tick must be recorded");

            final RecordedEvent event = ticks.getFirst();
            assertEquals(tick.id, event.getLong("tickId"));
            assertEquals(-1L, event.getLong("regionId"), "a fake tick has no region");
            assertTrue(event.getLong("startNanos") > 0L);
            assertTrue(event.getLong("endNanos") >= event.getLong("startNanos"));
            assertTrue(event.getLong("executedNanos") >= 0L);
            assertTrue(
                event.getLong("missedPeriods") >= 2L,
                "the handle is several periods behind and the event must say so: " + event
            );
            assertEquals(0, event.getInt("catchUpTicks"), "catch-up is off in this fund");
            assertTrue(event.getLong("skipped") >= 1L, "the stock body wrote periods off: " + event);
        }
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
    void theSwitchOffRecordsNothing() throws Exception {
        final List<RecordedEvent> ticks = Collections.synchronizedList(new ArrayList<>());
        try (RecordingStream stream = new RecordingStream()) {
            stream.onEvent("tuff.scheduler.Tick", ticks::add);
            stream.startAsync();
            Thread.sleep(100L);

            final KairosScheduler fixed = this.fund(new KairosKernel(1, 50, 0, 3, 0, false));
            final CatchUpTick tick = new CatchUpTick("silent", PERIOD, 0L, System::nanoTime, null, 0L).stopAfter(2);

            fixed.schedule(tick);
            fixed.start();
            assertTrue(tick.awaitBodies(1, 5_000L), "the tick must run");
            Thread.sleep(300L);

            assertTrue(ticks.isEmpty(), "the recording was enabled but the fund must not fill it: " + ticks);
            assertFalse(KairosGate.kernel().jfr());

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
