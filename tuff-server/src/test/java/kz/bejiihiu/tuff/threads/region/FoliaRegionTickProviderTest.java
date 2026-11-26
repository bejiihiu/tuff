package kz.bejiihiu.tuff.threads.region;

import ca.spottedleaf.moonrise.common.util.TickThread;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the three safety behaviours of the Folia reader: throttle, tick thread guard and "never throw".
 *
 * <p>No server is started. The world list and the clock are supplied; every world list here is empty, so
 * the walk itself never touches the regioniser. What is under test is the policy around the walk, which is
 * exactly the part that could hurt the server if it were wrong.
 */
class FoliaRegionTickProviderTest {

    private static final long FIVE_SECONDS = 5_000_000_000L;

    @Test
    void anEmptyServerReportsNoRegions() {
        final FoliaRegionTickProvider provider = new FoliaRegionTickProvider(List::of, () -> 0L);

        assertTrue(provider.read().isEmpty());
    }

    @Test
    void aSecondReadInsideTheSameSecondReusesTheFirst() {
        final AtomicInteger walks = new AtomicInteger();
        final AtomicLong clock = new AtomicLong();
        final FoliaRegionTickProvider provider = new FoliaRegionTickProvider(
            () -> {
                walks.incrementAndGet();
                return List.of();
            },
            clock::get
        );

        provider.read();
        clock.set(FIVE_SECONDS / 10L);
        provider.read();

        assertEquals(1, walks.get(), "a command run twice in a second must not walk every region twice");
    }

    @Test
    void theNextSecondReadsAgain() {
        final AtomicInteger walks = new AtomicInteger();
        final AtomicLong clock = new AtomicLong();
        final FoliaRegionTickProvider provider = new FoliaRegionTickProvider(
            () -> {
                walks.incrementAndGet();
                return List.of();
            },
            clock::get
        );

        provider.read();
        clock.set(FIVE_SECONDS);
        provider.read();

        assertEquals(2, walks.get());
    }

    @Test
    void aReadFromARegionTickThreadNeverWalksTheServer() throws InterruptedException {
        final AtomicInteger walks = new AtomicInteger();
        final AtomicLong clock = new AtomicLong();
        final FoliaRegionTickProvider provider = new FoliaRegionTickProvider(
            () -> {
                walks.incrementAndGet();
                return List.of();
            },
            clock::get
        );
        provider.read();
        // the throttle has expired, so only the tick thread guard can stop the second walk
        clock.set(FIVE_SECONDS);

        final AtomicReference<List<RegionReading>> answer = new AtomicReference<>();
        final Thread thread = new TickThread(() -> answer.set(provider.read()), "test-region-thread");
        thread.start();
        thread.join(5_000L);

        assertEquals(List.of(), answer.get());
        assertEquals(1, walks.get(), "the regioniser lock must not be taken from inside a tick");
    }

    @Test
    void aBrokenWorldSourceReportsNoRegionsInsteadOfThrowing() {
        final FoliaRegionTickProvider provider = new FoliaRegionTickProvider(
            () -> {
                throw new IllegalStateException("the server is shutting down");
            },
            () -> 0L
        );

        assertTrue(provider.read().isEmpty());
    }
}
