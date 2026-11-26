package kz.bejiihiu.tuff.compute.internal.util;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurationsTest {

    @Test
    void saturatesInsteadOfThrowingOnOversizedValues() {
        final Duration huge = Duration.ofSeconds(Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, Durations.toNanosSaturated(huge));
        assertEquals(Long.MAX_VALUE, Durations.toMillisSaturated(huge));
        assertEquals(Long.MIN_VALUE, Durations.toNanosSaturated(huge.negated()));
        assertEquals(Long.MIN_VALUE, Durations.toMillisSaturated(huge.negated()));
    }

    @Test
    void keepsOrdinaryValuesExact() {
        final Duration duration = Duration.ofSeconds(3L).plusNanos(7L);

        assertEquals(3_000_000_007L, Durations.toNanosSaturated(duration));
        assertEquals(3_000L, Durations.toMillisSaturated(duration));
        assertEquals(0L, Durations.toNanosSaturated(Duration.ZERO));
    }

    @Test
    void deadlineNeverWrapsAround() {
        final long now = System.nanoTime();

        assertEquals(now, Durations.deadline(now, 0L));
        assertEquals(now, Durations.deadline(now, -5L));
        assertEquals(now + 10L, Durations.deadline(now, 10L));
        assertEquals(Long.MAX_VALUE, Durations.deadline(Long.MAX_VALUE - 1L, 10L));
        assertTrue(Durations.deadline(now, Long.MAX_VALUE) >= now);
    }
}
