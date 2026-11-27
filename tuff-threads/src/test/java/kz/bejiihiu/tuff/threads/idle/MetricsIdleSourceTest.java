package kz.bejiihiu.tuff.threads.idle;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Pacing and inertness of the metrics producer.
 *
 * <p>The clock is a counter, so "the warm gap has passed" is an exact question instead of a race. The
 * pass is a counter too: what matters here is that idle runners get one offer per gap and that a
 * stopped layer or a disabled metrics policy gets none, not what a real snapshot contains.
 */
@Timeout(60)
class MetricsIdleSourceTest {

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private final AtomicInteger passes = new AtomicInteger();
    private final AtomicInteger running = new AtomicInteger(1);
    private final AtomicInteger enabled = new AtomicInteger(1);

    private MetricsIdleSource source() {
        return new MetricsIdleSource(
            () -> this.running.get() != 0,
            () -> this.enabled.get() != 0,
            this.passes::incrementAndGet,
            this.clock::get,
            500L
        );
    }

    @Test
    void onePassIsOfferedPerWarmGap() {
        final MetricsIdleSource source = this.source();

        final IdleWork first = source.poll();
        assertNotNull(first, "the first poll after start is due");
        first.run(() -> true);
        assertEquals(1, this.passes.get(), "the job is the snapshot pass");

        assertNull(source.poll(), "a second runner in the same window gets nothing");
        this.clock.addAndGet(499L);
        assertNull(source.poll(), "the gap is not over yet");
        this.clock.addAndGet(1L);
        assertNotNull(source.poll(), "the next window opens exactly at the gap");
    }

    @Test
    void aStoppedLayerOffersNothing() {
        final MetricsIdleSource source = this.source();
        this.running.set(0);

        assertNull(source.poll());
        this.clock.addAndGet(10_000L);
        assertNull(source.poll(), "a stopped layer must not keep sampling");
        assertEquals(0, this.passes.get());
    }

    @Test
    void disabledMetricsOfferNothing() {
        final MetricsIdleSource source = this.source();
        this.enabled.set(0);

        assertNull(source.poll());
        this.clock.addAndGet(10_000L);
        assertNull(source.poll(), "there is no measurement to refresh");
        assertEquals(0, this.passes.get());
    }
}
