package kz.bejiihiu.tuff.threads.scenario;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.MetricsConfig;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceSampler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 4: the sampler is stopped and started again while the pool is busy.
 *
 * <p>Stopping is {@link ResourceSampler#close()}: the last pass stays readable, the cache stops moving
 * and the probe is not called again. A restart is a fresh sampler over the same sources, which is what
 * the bootstrap builds after a stop; it measures again immediately. The sampler owns no thread, so
 * neither phase can leak one - the structural check at the end is the proof.
 */
@Timeout(120)
class SamplerRestartScenarioTest {

    @Test
    void aStoppedSamplerKeepsTheCacheAndARestartMeasuresAgain() {
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16,
            ScenarioFixtures.settings(false, 1, false, ScenarioFixtures.pressure(false), true),
            ScenarioFixtures.compute(1, -1, 2)
        );
        final Lease first = harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline()));
        final Lease second = harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline()));
        final ScenarioFixtures.CountingProbe probe = new ScenarioFixtures.CountingProbe(
            PoolId.COMPUTE, () -> ScenarioFixtures.Reading.of(3L, 2L)
        );
        final MetricsConfig config = new MetricsConfig(
            true, true, true, Duration.ofSeconds(1L), Duration.ofSeconds(30L)
        );
        final ResourceSampler sampler = ScenarioFixtures.sampler(
            harness, config, List.of(probe), new ScenarioFixtures.FakeCounters()
        );

        final ResourceSnapshot warmed = sampler.sampleNow();
        assertEquals(1, probe.calls(), "the first pass asks the probe");
        assertEquals(3L, warmed.pool(PoolId.COMPUTE).orElseThrow().queuedTasks());
        assertEquals(SaturationReport.Level.SATURATED,
            warmed.pool(PoolId.COMPUTE).orElseThrow().saturation().level(),
            "a saturated queue is what the pressure loop is under");

        harness.clock.advanceSeconds(1);
        final ResourceSnapshot measured = sampler.sampleNow();
        assertNotSame(warmed, measured, "a due pass replaces the cache");
        assertEquals(2, probe.calls());
        assertEquals(warmed.capturedAt().plusSeconds(1L), measured.capturedAt());

        sampler.close();
        assertFalse(sampler.enabled());
        final int callsAtStop = probe.calls();
        harness.clock.advanceSeconds(1);
        final ResourceSnapshot frozen = sampler.sampleNow();
        assertSame(measured, frozen, "a stopped sampler answers with the last real pass, not a new or zeroed one");
        assertEquals(measured.capturedAt(), frozen.capturedAt());
        assertEquals(callsAtStop, probe.calls(), "no pass may run after close");
        assertEquals(2, harness.controller.inUse(PoolId.COMPUTE), "the load is untouched by the sampler");

        // a restart is a fresh sampler over the same sources, exactly what a stopped and started
        // bootstrap builds; it must measure at once instead of inheriting the frozen cache
        final ResourceSampler restarted = ScenarioFixtures.sampler(
            harness, config, List.of(probe), new ScenarioFixtures.FakeCounters()
        );
        final ResourceSnapshot fresh = restarted.sampleNow();
        assertNotSame(frozen, fresh);
        assertEquals(callsAtStop + 1, probe.calls(), "the restarted sampler takes its own first pass");
        assertEquals(frozen.capturedAt().plusSeconds(1L), fresh.capturedAt());
        assertEquals(3L, fresh.pool(PoolId.COMPUTE).orElseThrow().queuedTasks());
        assertTrue(restarted.enabled());

        first.release();
        second.release();
        assertEquals(0, harness.controller.inUse(PoolId.COMPUTE));
    }

    @Test
    void theSamplerOwnsNoThreadAnExecutorOrATimer() {
        for (final Field field : ResourceSampler.class.getDeclaredFields()) {
            final Class<?> type = field.getType();
            assertFalse(Thread.class.isAssignableFrom(type), "unexpected thread field: " + field);
            assertFalse(ExecutorService.class.isAssignableFrom(type), "unexpected executor field: " + field);
            assertFalse(ScheduledExecutorService.class.isAssignableFrom(type), "unexpected executor field: " + field);
            assertFalse(Timer.class.isAssignableFrom(type), "unexpected timer field: " + field);
        }
        assertEquals(0, liveSamplerThreads(), "the sampler must not name or own a thread");
    }

    private static long liveSamplerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(thread -> thread.getName().startsWith("Tuff Sampler"))
            .count();
    }
}
