package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.kairos.KairosSnapshot.Degraded;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R5 behaviour: a tick that never gets a permit must be visible, without turning the log into a flood.
 *
 * <p>The detector is driven by a clock the test moves, so "the deadline passed 200 ms ago" and "the ten
 * second quiet window is over" cost no wall time. Ticks themselves still sleep in real time, because
 * that is what keeps a permit busy.
 */
@Timeout(120)
class KairosOverdueTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void newFactory() {
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        if (this.scheduler != null) {
            this.scheduler.halt();
            assertTrue(this.scheduler.join(15_000L), "halt must stop every thread of an elastic fund");
        }
        KairosGate.disarm();
    }

    @Test
    void aFullFundWarnsSaturatedOnceAndCountsTheSuppressedPasses() throws Exception {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler elastic = this.elastic(1, 2, clock);
        final CountDownLatch done = new CountDownLatch(3);
        final long base = clock.getAsLong();
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(this.tick("t" + index, base - millis(200L), done));
        }

        try (WarningCapture capture = WarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");
            elastic.start();

            assertTrue(capture.await(1, 5_000L), "a tick that waits 200 ms must be reported");
            final String first = capture.warnings().get(0);
            assertTrue(first.contains("saturated"), first);
            assertTrue(first.contains("2/2"), "the warning must carry live/max, was: " + first);
            assertTrue(first.contains("region unknown (tick"), "a fake tick has no region, was: " + first);

            Thread.sleep(300L);
            assertEquals(1, capture.warnings().size(), "the quiet window must swallow the following passes");

            clock.advance(TimeUnit.SECONDS.toNanos(11L));
            assertTrue(capture.await(2, 5_000L), "the next window must be allowed to warn again");
            final String second = capture.warnings().get(1);
            assertTrue(second.contains("suppressed"), "the next warning must say what it swallowed: " + second);
        }
        final KairosSnapshot snapshot = elastic.snapshot();
        assertTrue(snapshot.degradedReasons().contains(Degraded.QUEUED_OVERDUE));
        assertTrue(snapshot.overdueTicks() > 0, "the snapshot must show the same overdue ticks");
        assertTrue(snapshot.oldestOverdueMillis() >= 100L);
    }

    @Test
    void aFundThatStillHasRoomBlamesStarvationNotSaturation() throws Exception {
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler elastic = this.elastic(1, 4, clock);
        final CountDownLatch done = new CountDownLatch(3);
        final long base = clock.getAsLong();
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(this.tick("t" + index, base - millis(200L), done));
        }

        try (WarningCapture capture = WarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");
            elastic.start();

            assertTrue(capture.await(1, 5_000L), "the overdue tick must be reported");
            final String warning = capture.warnings().get(0);
            assertTrue(warning.contains("starvation"), warning);
            assertTrue(warning.contains("2/4"), "the warning must carry live/max, was: " + warning);
            assertFalse(warning.contains("saturated"), warning);
        }
    }

    @Test
    void aFixedFundHasNoDetectorAtAll() throws Exception {
        KairosGate.arm(true, new KairosTuning(false, 2, 5_000, 50, 20L));
        final MutableNanoClock clock = new MutableNanoClock(System.nanoTime());
        final KairosScheduler fixed = this.scheduler =
            new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline(), clock);
        final CountDownLatch done = new CountDownLatch(3);
        final long base = clock.getAsLong();
        for (int index = 0; index < 3; ++index) {
            fixed.schedule(this.tick("t" + index, base - millis(200L), done));
        }

        try (WarningCapture capture = WarningCapture.attach()) {
            Assumptions.assumeTrue(capture != null, "log4j2 core is not on the test classpath");
            fixed.start();
            Thread.sleep(400L);

            assertTrue(capture.warnings().isEmpty(), "a fixed fund has no supervisor and therefore no detector");
        }
        assertTrue(fixed.snapshot().overdueTicks() > 0, "the snapshot still reports the stale tick");
    }

    private KairosScheduler elastic(final int min, final int max, final MutableNanoClock clock) {
        KairosGate.arm(true, new KairosTuning(true, max, 5_000, 50, 20L));
        return this.scheduler = new KairosScheduler(
            min, this.threads, tick -> ((FakeTick) tick).deadline(), clock
        );
    }

    private FakeTick tick(final String label, final long deadline, final CountDownLatch done) {
        return new FakeTick(label, deadline, this.order, done).runFor(millis(1_500L));
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }

    /**
     * Records WARN and above from the Kairos logger, which is exactly the stream the no-spam promise is
     * about.
     */
    private static final class WarningCapture implements AutoCloseable {

        private final LoggerContext context;
        private final RecordingAppender appender;

        private WarningCapture(final LoggerContext context, final RecordingAppender appender) {
            this.context = context;
            this.appender = appender;
        }

        static @Nullable WarningCapture attach() {
            if (!(LogManager.getContext(false) instanceof LoggerContext context)) {
                return null;
            }
            final RecordingAppender appender = new RecordingAppender();
            appender.start();
            context.getRootLogger().addAppender(appender);
            context.updateLoggers();
            return new WarningCapture(context, appender);
        }

        boolean await(final int expected, final long millis) throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
            while (this.appender.events.size() < expected) {
                if (System.nanoTime() - deadline >= 0L) {
                    return false;
                }
                Thread.sleep(10L);
            }
            return true;
        }

        List<String> warnings() {
            return List.copyOf(this.appender.events);
        }

        @Override
        public void close() {
            this.context.getRootLogger().removeAppender(this.appender);
            this.context.updateLoggers();
            this.appender.stop();
        }

        private static final class RecordingAppender extends AbstractAppender {

            private final List<String> events = Collections.synchronizedList(new ArrayList<>());

            private RecordingAppender() {
                super("KairosWarningCapture", null, null, true, Property.EMPTY_ARRAY);
            }

            @Override
            public void append(final LogEvent event) {
                // lower intLevel is more severe: WARN and only what is worse than it
                if (event.getLevel().intLevel() > Level.WARN.intLevel()) {
                    return;
                }
                final String logger = event.getLoggerName();
                if (logger == null || !logger.equals("Tuff/Kairos")) {
                    return;
                }
                this.events.add(event.getMessage().getFormattedMessage());
            }
        }
    }
}
