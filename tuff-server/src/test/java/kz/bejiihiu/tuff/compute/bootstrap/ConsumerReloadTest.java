package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.config.ConfigDiff;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class ConsumerReloadTest {

    private static final ConsumerPolicy BASE = new ConsumerPolicy(
        true,
        ConsumerPolicy.ConcurrencyLimit.automatic(),
        -1,
        1.0d,
        OverflowPolicy.REJECT,
        Duration.ZERO
    );

    private static ConsumerPolicy weighted(final double weight) {
        return new ConsumerPolicy(BASE.enabled(), BASE.concurrencyLimit(), BASE.maxQueuedTasks(), weight,
            BASE.overflowPolicy(), BASE.maxQueueWait());
    }

    @Test
    void aReloadOnlyReplacesPoliciesThatChangedInTheConfig() throws Exception {
        final ComputeSettings before = settings(BASE, Map.of(), OverflowPolicy.REJECT);
        final AsyncComputeScheduler scheduler = start(before);
        final LimitedConsumerScheduler guarded = LimitedConsumerScheduler.wrap(scheduler);
        try {
            // identical to the default on purpose: the reload must tell "registered by a plugin" from
            // "registered from the config" even when the two policies look the same
            guarded.registerConsumer(ComputeConsumer.of("plugin-own", BASE));

            final ComputeSettings after = settings(weighted(2.0d), Map.of(), OverflowPolicy.REJECT);
            final ConsumerReload.Outcome outcome = ConsumerReload.apply(guarded, before, after);

            assertEquals(List.of("default"), outcome.applied());
            assertEquals(List.of(), outcome.deferred());
            assertEquals(weighted(2.0d), guarded.consumer("default").orElseThrow().policy());
            assertEquals(BASE, guarded.consumer("plugin-own").orElseThrow().policy());
        } finally {
            stop(scheduler);
        }
    }

    @Test
    void theQueueOverflowDefaultReachesTheDefaultAndDynamicConsumersOnReload() throws Exception {
        final ComputeSettings before = settings(BASE, Map.of(), OverflowPolicy.REJECT);
        final AsyncComputeScheduler scheduler = start(before);
        final LimitedConsumerScheduler guarded = LimitedConsumerScheduler.wrap(scheduler);
        try {
            assertEquals(1, guarded
                .submit("dynamic-one", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> 1)
                .get(10L, TimeUnit.SECONDS));
            assertEquals(OverflowPolicy.REJECT, guarded.consumer("dynamic-one").orElseThrow().policy().overflowPolicy());

            final ComputeSettings after = settings(BASE, Map.of(), OverflowPolicy.DROP_OLDEST);
            final ConsumerReload.Outcome outcome = ConsumerReload.apply(guarded, before, after);

            assertTrue(outcome.applied().contains("default"), outcome.applied().toString());
            assertTrue(outcome.applied().contains("dynamic-one"), outcome.applied().toString());
            assertEquals(OverflowPolicy.DROP_OLDEST,
                guarded.consumer("default").orElseThrow().policy().overflowPolicy());
            assertEquals(OverflowPolicy.DROP_OLDEST,
                guarded.consumer("dynamic-one").orElseThrow().policy().overflowPolicy());
        } finally {
            stop(scheduler);
        }
    }

    @Test
    void anOverrideChangedForAnUnregisteredConsumerIsDeferred() throws Exception {
        final ComputeSettings before = settings(BASE, Map.of(), OverflowPolicy.REJECT);
        final AsyncComputeScheduler scheduler = start(before);
        try {
            final ComputeSettings after = settings(BASE, Map.of("ghost", ConsumerPolicy.absolute(2)), OverflowPolicy.REJECT);
            final ConsumerReload.Outcome outcome = ConsumerReload.apply(scheduler, before, after);

            assertEquals(List.of(), outcome.applied());
            assertEquals(List.of("ghost"), outcome.deferred());
            assertTrue(scheduler.consumer("ghost").isEmpty());
        } finally {
            stop(scheduler);
        }
    }

    @Test
    void anExplicitOverrideStillReachesAPluginRegisteredConsumer() throws Exception {
        final ComputeSettings before = settings(BASE, Map.of(), OverflowPolicy.REJECT);
        final AsyncComputeScheduler scheduler = start(before);
        final LimitedConsumerScheduler guarded = LimitedConsumerScheduler.wrap(scheduler);
        try {
            guarded.registerConsumer(ComputeConsumer.of("tuned", BASE));

            final ComputeSettings after = settings(BASE, Map.of("tuned", ConsumerPolicy.absolute(3)), OverflowPolicy.REJECT);
            final ConsumerReload.Outcome outcome = ConsumerReload.apply(guarded, before, after);

            assertEquals(List.of("tuned"), outcome.applied());
            assertEquals(ConsumerPolicy.absolute(3), guarded.consumer("tuned").orElseThrow().policy());
        } finally {
            stop(scheduler);
        }
    }

    @Test
    void deferredPathsDoNotClaimThatAnUnregisteredConsumerNeedsARestart() {
        final ConfigDiff diff = new ConfigDiff(
            List.of(
                "async-compute.consumers.ghost.max-concurrent",
                "async-compute.queue.overflow-policy",
                "async-compute.workers.threads"
            ),
            List.of()
        );

        assertEquals(
            List.of("async-compute.queue.overflow-policy", "async-compute.workers.threads"),
            ConsumerReload.deferredPaths(diff, List.of(), List.of("ghost"))
        );
        assertEquals(
            List.of("async-compute.workers.threads"),
            ConsumerReload.deferredPaths(diff, List.of("ghost"), List.of())
        );
    }

    private static ComputeSettings settings(
        final ConsumerPolicy defaultPolicy,
        final Map<String, ConsumerPolicy> overrides,
        final OverflowPolicy overflow
    ) {
        final ComputeSettings defaults = ComputeSettings.defaults();
        return new ComputeSettings(
            true,
            new ComputeSettings.Workers(2, 1, 2, Duration.ofSeconds(5L), "Tuff Compute Reload Test"),
            new ComputeSettings.QueueSettings(64, overflow),
            defaults.scheduling(),
            new ComputeSettings.Consumers(defaultPolicy, overrides, true),
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
