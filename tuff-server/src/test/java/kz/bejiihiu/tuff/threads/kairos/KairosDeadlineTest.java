package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.common.util.TimeUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A tick without a scheduled start, and the park a runner derives from it.
 *
 * <p>{@link TimeUtil#DEADLINE_NOT_SET} is {@link Long#MIN_VALUE}. Subtracting it from
 * {@link System#nanoTime()} overflows into a number of nanoseconds that parks the thread for about
 * three centuries while it holds the tick's permit, and the watchdog would be the only thing that
 * noticed. The parent contract already says {@code schedule} refuses such a tick; this class holds the
 * port to that contract and bounds the park anyway, because the deadline value is what a runner turns
 * into a park and nothing else checks it.
 */
@Timeout(120)
class KairosDeadlineTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void newFactory() {
        KairosGate.arm(true);
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        if (this.scheduler != null) {
            this.scheduler.halt();
            assertTrue(this.scheduler.join(10_000L), "halt must stop every thread of the fund");
        }
        KairosGate.disarm();
    }

    @Test
    void scheduleRefusesATickWithoutADeadline() {
        final KairosScheduler fund = this.scheduler =
            new KairosScheduler(1, this.threads, tick -> TimeUtil.DEADLINE_NOT_SET);
        final FakeTick tick = new FakeTick("no-deadline", 0L, this.order, new CountDownLatch(1));

        final IllegalStateException refused =
            assertThrows(IllegalStateException.class, () -> fund.schedule(tick));

        assertTrue(refused.getMessage().contains("no scheduled start"), refused.getMessage());
        assertEquals(0, fund.scheduledTicks(), "a refused tick must not leave an entry behind");
        assertTrue(fund.join(1_000L), "nothing was started, so there is nothing to wait for");
        assertFalse(this.threads.anyAlive());
    }

    @Test
    void aParkDerivedFromAnUnsetDeadlineIsBounded() {
        final long now = System.nanoTime();

        assertEquals(
            TimeUnit.SECONDS.toNanos(1L),
            KairosScheduler.parkNanos(TimeUtil.DEADLINE_NOT_SET, now),
            "an unset deadline must never park the runner for centuries"
        );
        assertEquals(millis(50L), KairosScheduler.parkNanos(now + millis(50L), now),
            "a near deadline is honoured as is");
        assertEquals(0L, KairosScheduler.parkNanos(now - 1L, now), "a passed deadline means no park");
        assertEquals(
            TimeUnit.SECONDS.toNanos(1L),
            KairosScheduler.parkNanos(now + TimeUnit.DAYS.toNanos(1L), now),
            "a very distant deadline is rechecked once a second"
        );
    }

    private static long millis(final long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }
}
