package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per consumer concurrency limits and live policy updates.
 */
@Timeout(60)
class EngineQuotaTest {

    @Test
    void oneConsumerCannotOccupyTheWholePool() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("narrow", ConsumerPolicy.absolute(1)));
            compute.registerConsumer(ComputeConsumer.of("wide", ConsumerPolicy.defaults()));
            final CompletableFuture<String> narrowRunning = compute.submit("narrow", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "narrow-1";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> narrowQueued = compute.submit("narrow", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "narrow-2");
            final CompletableFuture<String> wide = compute.submit("wide", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "wide");

            assertEquals("wide", wide.get(10L, TimeUnit.SECONDS));
            assertFalse(narrowQueued.isDone(), "the narrow consumer is at its limit of one");
            release.countDown();
            assertEquals("narrow-1", narrowRunning.get(10L, TimeUnit.SECONDS));
            assertEquals("narrow-2", narrowQueued.get(10L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void raisingTheLimitOnTheFlyLetsQueuedWorkStart() throws Exception {
        final CountDownLatch first = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("dynamic", ConsumerPolicy.absolute(1)));
            final CompletableFuture<String> running = compute.submit("dynamic", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                first.countDown();
                release.await();
                return "first";
            });
            assertTrue(first.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> queued = compute.submit("dynamic", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "second");
            Thread.sleep(50L);
            assertFalse(queued.isDone());

            compute.updateConsumerPolicy("dynamic", ConsumerPolicy.absolute(2));
            assertEquals("second", queued.get(10L, TimeUnit.SECONDS));
            release.countDown();
            assertEquals("first", running.get(10L, TimeUnit.SECONDS));
            assertEventually("the raised limit is visible", Duration.ofSeconds(2), () ->
                compute.metrics().consumer("dynamic").orElseThrow().concurrencyLimit() == 2);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void loweringTheLimitNeverInterruptsRunningWork() throws Exception {
        final CountDownLatch bothRunning = new CountDownLatch(2);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final CountDownLatch releaseSecond = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(3).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("shrinking", ConsumerPolicy.absolute(2)));
            final CompletableFuture<String> first = compute.submit("shrinking", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                bothRunning.countDown();
                releaseFirst.await();
                return "first";
            });
            final CompletableFuture<String> second = compute.submit("shrinking", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                bothRunning.countDown();
                releaseSecond.await();
                return "second";
            });
            assertTrue(bothRunning.await(10L, TimeUnit.SECONDS));

            compute.updateConsumerPolicy("shrinking", ConsumerPolicy.absolute(1));
            final CompletableFuture<String> third = compute.submit("shrinking", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "third");
            assertFalse(third.isDone(), "the consumer is already above its new limit");
            assertFalse(first.isDone());
            assertFalse(second.isDone());

            releaseFirst.countDown();
            assertEquals("first", first.get(10L, TimeUnit.SECONDS));
            Thread.sleep(50L);
            assertFalse(third.isDone(), "one task still runs above the new limit");
            releaseSecond.countDown();
            assertEquals("second", second.get(10L, TimeUnit.SECONDS));
            assertEquals("third", third.get(10L, TimeUnit.SECONDS));
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            compute.shutdown();
        }
    }

    @Test
    void relativeLimitFollowsThePoolSize() {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(4).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("relative", ConsumerPolicy.relative(0.5d)));
            assertEventually("quota resolves the relative limit", Duration.ofSeconds(2), () ->
                compute.metrics().consumer("relative").orElseThrow().concurrencyLimit() == 2);
        } finally {
            compute.shutdown();
        }
    }
}
