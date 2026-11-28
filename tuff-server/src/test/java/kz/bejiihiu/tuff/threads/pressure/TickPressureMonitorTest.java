package kz.bejiihiu.tuff.threads.pressure;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The monitor is the glue, so these tests check the glue: what reaches the sink, what the snapshot
 * says, and that no timer or thread was smuggled in to make it work.
 *
 * <p>The clock is the argument, not the wall clock, so a test that means "two seconds later" gets
 * exactly that.
 */
@Timeout(120)
class TickPressureMonitorTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1L);

    @Test
    void aDisabledPolicyCallsNeitherTheCeilingNorTheSink() {
        final RecordingSink sink = new RecordingSink(2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy(false), () -> 8);
        monitor.bind(sink);

        for (int second = 0; second < 20; second++) {
            monitor.evaluate(lagging(), second * SECOND);
        }

        assertEquals(0, sink.maxReserveCalls);
        assertTrue(sink.reserves.isEmpty());
        assertNull(monitor.snapshot(), "a disabled policy publishes nothing");
    }

    @Test
    void everyReserveChangeReachesTheSinkWithTheBudgetAfterIt() {
        final RecordingSink sink = new RecordingSink(2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy(true), () -> 8);
        monitor.bind(sink);

        monitor.evaluate(lagging(), 0L);
        assertEquals(List.of(1), sink.reserves);
        final TickPressureSnapshot entered = monitor.snapshot();
        assertNotNull(entered);
        assertEquals(7, entered.budget(), "budget = usable 8 - reserve 1");
        assertEquals(2, entered.maxReserve());
        assertEquals(8, entered.usableCores());
        assertEquals(TickPressureController.State.PRESSURE, entered.state());

        monitor.evaluate(lagging(), 5 * SECOND);
        assertEquals(List.of(1, 2), sink.reserves);
        assertEquals(6, monitor.snapshot().budget());

        // calm windows are counted from here; three of them plus the cooldown release the first core
        for (int second = 6; second <= 9; second++) {
            monitor.evaluate(calm(), second * SECOND);
        }
        assertEquals(List.of(1, 2), sink.reserves, "four calm seconds but the cooldown is not over");
        monitor.evaluate(calm(), 10 * SECOND);
        assertEquals(List.of(1, 2, 1), sink.reserves);
        assertEquals(7, monitor.snapshot().budget());

        // and the last core goes after another set of windows and a cooldown
        for (int second = 11; second <= 14; second++) {
            monitor.evaluate(calm(), second * SECOND);
        }
        assertEquals(List.of(1, 2, 1), sink.reserves, "the second step needs its own cooldown");
        monitor.evaluate(calm(), 15 * SECOND);
        assertEquals(List.of(1, 2, 1, 0), sink.reserves);
        assertEquals(TickPressureController.State.NORMAL, monitor.snapshot().state());
        assertEquals(8, monitor.snapshot().budget());
    }

    @Test
    void anUnboundMonitorStillPublishesTheDecision() {
        final TickPressureMonitor monitor = new TickPressureMonitor(policy(true), () -> 8);

        monitor.evaluate(lagging(), 0L);

        final TickPressureSnapshot snapshot = monitor.snapshot();
        assertNotNull(snapshot, "the command has to see the decision even when nothing can apply it");
        assertEquals(1, snapshot.reserve());
        assertEquals(7, snapshot.budget());
    }

    @Test
    void theBudgetNeverFallsBelowOneCore() {
        assertEquals(1, TickPressureMonitor.budgetFor(1, 5));
        assertEquals(1, TickPressureMonitor.budgetFor(1, 0));
        assertEquals(3, TickPressureMonitor.budgetFor(5, 2));
    }

    @Test
    void theSignalIsReadFromTheRegionPoolOfTheSnapshot() {
        final RecordingSink sink = new RecordingSink(2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy(true), () -> 8);
        monitor.bind(sink);

        monitor.onSample(snapshotWithRegions(), 0L);

        assertEquals(List.of(1), sink.reserves, "50% of four regions lagging must start pressure");
        assertEquals(4, monitor.snapshot().signal().regions());
        assertEquals(0.5d, monitor.snapshot().signal().lagFraction());
    }

    @Test
    void aSnapshotWithoutRegionsChangesNothing() {
        final RecordingSink sink = new RecordingSink(2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy(true), () -> 8);
        monitor.bind(sink);

        monitor.onSample(snapshotWithoutRegions(), 0L);

        assertTrue(sink.reserves.isEmpty(), "no region reading is not zero lag");
        assertNotNull(monitor.snapshot());
        assertEquals(TickPressureController.Reason.SIGNAL_UNAVAILABLE, monitor.snapshot().reason());
        assertEquals(0, monitor.snapshot().reserve());
    }

    @Test
    void aStoppedMonitorIgnoresLaterPasses() {
        final RecordingSink sink = new RecordingSink(2);
        final TickPressureMonitor monitor = new TickPressureMonitor(policy(true), () -> 8);
        monitor.bind(sink);
        monitor.evaluate(lagging(), 0L);
        assertEquals(List.of(1), sink.reserves);

        monitor.stop();
        monitor.evaluate(lagging(), 10 * SECOND);
        monitor.onSample(snapshotWithRegions(), 20 * SECOND);

        assertEquals(List.of(1), sink.reserves, "a stopped monitor must not resize a pool during shutdown");
        assertFalse(monitor.isRunning());
    }

    @Test
    void theMonitorOwnsNoThreadNoExecutorAndNoTimer() {
        for (final Field field : TickPressureMonitor.class.getDeclaredFields()) {
            final Class<?> type = field.getType();
            assertFalse(Thread.class.isAssignableFrom(type), "unexpected thread field: " + field);
            assertFalse(ExecutorService.class.isAssignableFrom(type), "unexpected executor field: " + field);
            assertFalse(ScheduledExecutorService.class.isAssignableFrom(type), "unexpected executor field: " + field);
            assertFalse(Timer.class.isAssignableFrom(type), "unexpected timer field: " + field);
        }
        for (final Field field : TickPressureController.class.getDeclaredFields()) {
            final Class<?> type = field.getType();
            assertFalse(Thread.class.isAssignableFrom(type), "unexpected thread field: " + field);
            assertFalse(ExecutorService.class.isAssignableFrom(type), "unexpected executor field: " + field);
        }
    }

    private static TickSignal lagging() {
        return new TickSignal(true, 4, 0.5d, 16.0d, 12.0d);
    }

    private static TickSignal calm() {
        return new TickSignal(true, 4, 0.0d, 20.0d, 2.0d);
    }

    private static ThreadManagementSettings.AsyncCompute.TickPressure policy(final boolean enabled) {
        return new ThreadManagementSettings.AsyncCompute.TickPressure(enabled, 0.25d, 19.0d, 19.5d, 3, 5, 2, 1);
    }

    private static ResourceSnapshot snapshotWithRegions() {
        return snapshot(Optional.of(new RegionPoolMetrics(4, 4.0d, 9.0d, 4.0d, 11.0d, 17.0d, 2, 0, 0, 16)));
    }

    private static ResourceSnapshot snapshotWithoutRegions() {
        return snapshot(Optional.empty());
    }

    private static ResourceSnapshot snapshot(final Optional<RegionPoolMetrics> regions) {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region tick execution"
        );
        final PoolSnapshot pool = new PoolSnapshot(
            spec, 4, 0, 0, 4, -1L, -1L, -1L, -1L, -1L, -1.0d,
            SaturationReport.unknown(PoolId.REGION_EXECUTION),
            List.of(DegradedReason.OBSERVE_ONLY),
            regions
        );
        return new ResourceSnapshot(
            Instant.EPOCH, 8, 1, 4, 0, 4, 4, 0.0d, 0.0d, -1.0d, List.of(pool), List.of(),
            Set.of(DegradedReason.OBSERVE_ONLY)
        );
    }

    private static final class RecordingSink implements PressureBudgetSink {

        private final int max;
        private final List<Integer> reserves = new ArrayList<>();
        private int maxReserveCalls;

        private RecordingSink(final int max) {
            this.max = max;
        }

        @Override
        public int maxReserve(final int usableCores) {
            this.maxReserveCalls++;
            return Math.min(this.max, Math.max(0, usableCores - 1));
        }

        @Override
        public void reserveChanged(final int reserve) {
            this.reserves.add(reserve);
        }
    }
}
