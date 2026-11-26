package kz.bejiihiu.tuff.compute.internal.queue.impl;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removing a quota while tasks are alive must never zero the live count, even when a policy update
 * races the removal.
 */
@Timeout(120)
class AtomicQuotaControllerRetirementTest {

    @Test
    void removalWhileActiveKeepsTheSameQuotaObjectAndCanBeRetried() {
        final AtomicQuotaController controller = new AtomicQuotaController(8);
        controller.updateConsumerPolicy("x", ConsumerPolicy.defaults());
        assertTrue(controller.tryAcquire("x"));

        assertThrows(IllegalStateException.class, () -> controller.removeConsumer("x"));
        assertEquals(1, controller.activeTasks("x"), "the live slot must still be counted after a refused removal");

        // a re-registration revives the same accounting object instead of replacing it
        controller.updateConsumerPolicy("x", ConsumerPolicy.absolute(2));
        assertEquals(1, controller.activeTasks("x"));

        controller.release("x");
        controller.removeConsumer("x");
        assertEquals(0, controller.activeTasks("x"));
        assertEquals(0, controller.limit("x"));
    }

    @Test
    void retirementRacingAPolicyUpdateNeverLosesTheLiveCount() throws Exception {
        final AtomicQuotaController controller = new AtomicQuotaController(8);
        final int iterations = 5_000;
        final AtomicInteger mismatches = new AtomicInteger();
        final CyclicBarrier start = new CyclicBarrier(3);
        final AtomicInteger sequence = new AtomicInteger();

        final Thread remover = Thread.ofPlatform().daemon(true).unstarted(() -> {
            await(start);
            for (int index = 0; index < iterations; index++) {
                controller.removeConsumer("c" + index);
            }
        });
        final Thread updater = Thread.ofPlatform().daemon(true).unstarted(() -> {
            await(start);
            for (int index = 0; index < iterations; index++) {
                controller.updateConsumerPolicy("c" + index, ConsumerPolicy.defaults());
            }
        });
        final Thread driver = Thread.ofPlatform().daemon(true).unstarted(() -> {
            await(start);
            for (int index = 0; index < iterations; index++) {
                final String consumerId = "c" + index;
                controller.updateConsumerPolicy(consumerId, ConsumerPolicy.defaults());
                if (controller.tryAcquire(consumerId)) {
                    // the acquire must survive both the update and the removal attempt
                    if (controller.activeTasks(consumerId) != 1) {
                        mismatches.incrementAndGet();
                    }
                    controller.release(consumerId);
                } else if (controller.activeTasks(consumerId) > 0) {
                    mismatches.incrementAndGet();
                }
                sequence.incrementAndGet();
            }
        });

        remover.start();
        updater.start();
        driver.start();
        remover.join(TimeUnit.SECONDS.toMillis(60));
        updater.join(TimeUnit.SECONDS.toMillis(60));
        driver.join(TimeUnit.SECONDS.toMillis(60));
        assertTrue(!remover.isAlive() && !updater.isAlive() && !driver.isAlive(), "race threads must finish");

        assertEquals(0, mismatches.get(), "a live acquire was lost while the consumer was being retired");
        assertEquals(iterations, sequence.get());
        assertEquals(0, controller.activeTasks());
    }

    private static void await(final CyclicBarrier barrier) {
        try {
            barrier.await(30L, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("barrier failed", failure);
        }
    }
}
