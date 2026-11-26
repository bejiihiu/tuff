package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue.OfferResult;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Retirement and close races of the bounded queue: an offer must never resurrect a consumer stream
 * or land in a closed queue.
 */
@Timeout(30)
class BoundedComputeQueueRetirementTest {

    private TestSupport.TestClock clock;
    private long sequence;

    @BeforeEach
    void setUp() {
        this.clock = new TestSupport.TestClock();
        this.sequence = 0L;
    }

    @Test
    void offerRegisteredRefusesAnUnknownConsumerAndCreatesNoStream() {
        final BoundedComputeQueue queue = this.queue(8);

        assertEquals(OfferResult.CONSUMER_UNKNOWN, queue.offerRegistered(this.entry("ghost")));
        assertEquals(0, queue.size());
        assertEquals(0, queue.size("ghost"));
        assertNull(queue.peek());
        assertNull(queue.poll(admitted -> true));

        // the plain offer keeps its embedder contract: an unknown id is registered implicitly
        assertEquals(OfferResult.ACCEPTED, queue.offer(this.entry("ghost")));
        assertEquals(1, queue.size("ghost"));
    }

    @Test
    void inFlightOfferForARetiredConsumerIsRefusedInsteadOfCreatingAGhostStream() throws Exception {
        final BoundedComputeQueue queue = this.queue(8);
        queue.registerConsumer("x", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));

        final CountDownLatch paused = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        queue.setInsertionGateForTesting(() -> {
            paused.countDown();
            try {
                resume.await(10L, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        final QueueEntry entry = this.entry("x");
        final OfferResult[] result = new OfferResult[1];
        final Thread offer = Thread.ofPlatform().daemon(true).start(() -> result[0] = queue.offerRegistered(entry));

        assertEquals(true, paused.await(10L, TimeUnit.SECONDS));
        queue.unregisterConsumer("x");
        resume.countDown();
        offer.join(10_000L);

        assertEquals(OfferResult.CONSUMER_UNKNOWN, result[0]);
        assertEquals(0, queue.size());
        assertEquals(0, queue.size("x"));
        assertNull(queue.peek());
    }

    @Test
    void reRegistrationDoesNotResurrectAnOfferThatLostTheRetirementRace() throws Exception {
        final BoundedComputeQueue queue = this.queue(8);
        queue.registerConsumer("x", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));

        final CountDownLatch paused = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        queue.setInsertionGateForTesting(() -> {
            paused.countDown();
            try {
                resume.await(10L, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        final OfferResult[] result = new OfferResult[1];
        final Thread offer = Thread.ofPlatform()
            .daemon(true)
            .start(() -> result[0] = queue.offerRegistered(this.entry("x")));

        assertEquals(true, paused.await(10L, TimeUnit.SECONDS));
        queue.unregisterConsumer("x");
        queue.registerConsumer("x", TestSupport.policy(-1, 1.0d, OverflowPolicy.REJECT));
        resume.countDown();
        offer.join(10_000L);

        assertEquals(OfferResult.CONSUMER_UNKNOWN, result[0]);
        assertEquals(0, queue.size("x"), "the revived registration must not receive the old work");
    }

    @Test
    void anEntryInsertedAfterCloseAndClearIsUnlinkedAndReportedClosed() {
        final BoundedComputeQueue queue = this.queue(8);
        queue.setInsertionGateForTesting(() -> {
            queue.close();
            queue.clear();
        });

        assertEquals(OfferResult.CLOSED, queue.offer(this.entry("a")));
        assertEquals(0, queue.size());
        assertEquals(0, queue.snapshot().accepted());
        assertNull(queue.poll(admitted -> true));
    }

    @Test
    void offerAfterCloseIsRefusedEvenWithoutTheGate() {
        final BoundedComputeQueue queue = this.queue(8);
        queue.close();

        assertEquals(OfferResult.CLOSED, queue.offer(this.entry("a")));
        assertEquals(OfferResult.CLOSED, queue.offerRegistered(this.entry("a")));
        assertEquals(0, queue.size());
    }

    private BoundedComputeQueue queue(final int capacity) {
        return new BoundedComputeQueue(
            capacity,
            TestSupport.scheduling(ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR, false, 1, false),
            ConsumerPolicy.defaults(),
            this.clock
        );
    }

    private QueueEntry entry(final String consumerId) {
        this.sequence++;
        return TestSupport.entry(this.sequence, consumerId, ComputePriority.NORMAL, this.sequence, this.clock.nanoTime());
    }
}
