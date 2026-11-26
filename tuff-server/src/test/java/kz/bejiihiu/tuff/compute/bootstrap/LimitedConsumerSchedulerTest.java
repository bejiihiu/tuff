package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class LimitedConsumerSchedulerTest {

    @Test
    void dynamicConsumerIdsAreCappedAndTheRefusalLeavesNoState() throws Exception {
        final LimitedConsumerScheduler limited = wrap(start(settings(Map.of())));
        try {
            for (int index = 0; index < LimitedConsumerScheduler.MAX_DYNAMIC_CONSUMERS; index++) {
                final int value = index;
                assertEquals(value, limited
                    .submit("dynamic-" + index, ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> value)
                    .get(10L, TimeUnit.SECONDS));
            }
            assertEquals(LimitedConsumerScheduler.MAX_DYNAMIC_CONSUMERS, limited.dynamicConsumerCount());

            final ComputeRejectedException rejection = assertThrows(ComputeRejectedException.class,
                () -> limited.submit("one-too-many", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> 0));

            assertEquals(ComputeRejectedException.Reason.CONSUMER_UNKNOWN, rejection.reason());
            assertEquals("one-too-many", rejection.consumerId());
            assertTrue(rejection.getMessage().contains(Integer.toString(LimitedConsumerScheduler.MAX_DYNAMIC_CONSUMERS)),
                rejection.getMessage());
            assertTrue(limited.consumer("one-too-many").isEmpty(), "a refused id must not be registered");
            assertTrue(limited.consumer("dynamic-0").isPresent(), "existing consumers must survive the refusal");
            assertEquals(7, limited
                .submit("dynamic-0", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> 7)
                .get(10L, TimeUnit.SECONDS));
        } finally {
            stop(limited);
        }
    }

    @Test
    void configuredConsumerIdsDoNotCountAgainstTheDynamicLimit() throws Exception {
        final ConsumerPolicy extra = ConsumerPolicy.absolute(1);
        final LimitedConsumerScheduler limited = wrap(start(settings(Map.of("configured-extra", extra))));
        try {
            assertEquals(0, limited.dynamicConsumerCount());
            assertEquals(1, limited
                .submit("configured-extra", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> 1)
                .get(10L, TimeUnit.SECONDS));
            assertEquals(0, limited.dynamicConsumerCount());
        } finally {
            stop(limited);
        }
    }

    @Test
    void unregisteringADynamicConsumerFreesItsSlot() throws Exception {
        final LimitedConsumerScheduler limited = wrap(start(settings(Map.of())));
        try {
            assertEquals(1, limited
                .submit("temporary", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> 1)
                .get(10L, TimeUnit.SECONDS));
            assertEquals(1, limited.dynamicConsumerCount());

            assertTrue(limited.unregisterConsumer("temporary"));
            assertEquals(0, limited.dynamicConsumerCount());
        } finally {
            stop(limited);
        }
    }

    private static LimitedConsumerScheduler wrap(final AsyncComputeScheduler scheduler) {
        return LimitedConsumerScheduler.wrap(scheduler);
    }

    private static ComputeSettings settings(final Map<String, ConsumerPolicy> overrides) {
        final ComputeSettings defaults = ComputeSettings.defaults();
        return new ComputeSettings(
            true,
            new ComputeSettings.Workers(2, 1, 2, Duration.ofSeconds(5L), "Tuff Compute Limit Test"),
            new ComputeSettings.QueueSettings(1024, defaults.queue().overflowPolicy()),
            defaults.scheduling(),
            new ComputeSettings.Consumers(ConsumerPolicy.defaults(), overrides, true),
            defaults.metrics(),
            defaults.adaptive(),
            Duration.ofSeconds(1L)
        );
    }

    private static AsyncComputeScheduler start(final ComputeSettings settings) {
        final AsyncComputeScheduler scheduler = ComputeSchedulers.create(settings, ComputeClock.system(), 2);
        scheduler.start();
        return scheduler;
    }

    private static void stop(final AsyncComputeScheduler scheduler) throws InterruptedException {
        scheduler.shutdown(Duration.ZERO);
        assertTrue(scheduler.awaitTermination(Duration.ofSeconds(10L)));
    }
}
