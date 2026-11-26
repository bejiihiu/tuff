package kz.bejiihiu.tuff.threads.internal.impl.events;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JfrThreadSinkTest {

    @TempDir
    Path tempDir;

    @Test
    void recordsEveryThreadEventWithItsFields() throws IOException {
        try (Recording recording = new Recording()) {
            recording.enable(PoolStateEvent.class);
            recording.enable(ThreadCreatedEvent.class);
            recording.enable(ThreadTerminatedEvent.class);
            recording.enable(SaturationEvent.class);
            recording.enable(BudgetChangedEvent.class);
            recording.enable(UnknownThreadsEvent.class);
            recording.enable(LeaseTimeoutEvent.class);
            recording.start();

            final JfrThreadSink sink = new JfrThreadSink();
            assertTrue(sink.enabled());

            sink.poolState(pool());
            sink.threadCreated(new ThreadSnapshot(42L, "compute-1", PoolId.COMPUTE, Thread.State.RUNNABLE, true, 3,
                1_500L));
            sink.threadTerminated(new ThreadSnapshot(43L, "compute-2", PoolId.COMPUTE, Thread.State.TERMINATED, true, 3,
                2_500L));
            sink.saturation(new SaturationReport(PoolId.COMPUTE, 0.5d, 0.25d, 10L, 0L,
                SaturationReport.Level.NORMAL));
            sink.budgetChanged(PoolId.COMPUTE, ResourceBudget.elastic(1.0d, 0), ResourceBudget.fixed(2, 1));
            sink.unregisteredThread(77L, "stray-worker");
            sink.leaseTimedOut(LeaseRequest.immediate(PoolId.COMPUTE, 9_000L));

            recording.stop();

            final List<RecordedEvent> events = this.readEvents(recording, "threads.jfr");

            final RecordedEvent state = single(events, "tuff.threads.PoolState");
            assertEquals("compute", state.getString("poolId"));
            assertEquals("COMPUTE", state.getString("poolKind"));
            assertEquals("L2", state.getString("coverage"));
            assertEquals(4, state.getInt("aliveThreads"));
            assertEquals(2, state.getInt("coreThreads"));
            assertEquals(3, state.getInt("activeThreads"));
            assertEquals(1, state.getInt("idleThreads"));
            assertEquals(12L, state.getLong("queuedTasks"));
            assertEquals(100L, state.getLong("queueCapacity"));
            assertEquals(0.25d, state.getDouble("cpuLoad"), 1.0e-9d);
            assertEquals("BUSY", state.getString("saturation"));
            assertEquals("METRICS_UNAVAILABLE", state.getString("degradedReasons"));

            final RecordedEvent created = single(events, "tuff.threads.ThreadCreated");
            assertEquals(42L, created.getLong("threadId"));
            assertEquals("compute-1", created.getString("threadName"));
            assertEquals("compute", created.getString("poolId"));
            assertTrue(created.getBoolean("daemon"));
            assertEquals(3, created.getInt("priority"));

            final RecordedEvent terminated = single(events, "tuff.threads.ThreadTerminated");
            assertEquals(43L, terminated.getLong("threadId"));
            assertEquals("TERMINATED", terminated.getString("state"));
            assertEquals(2_500L, terminated.getLong("cpuTimeNanos"));

            final RecordedEvent saturation = single(events, "tuff.threads.Saturation");
            assertEquals("compute", saturation.getString("poolId"));
            assertEquals("NORMAL", saturation.getString("level"));
            assertEquals(0.5d, saturation.getDouble("utilisation"), 1.0e-9d);
            assertEquals(0.25d, saturation.getDouble("queuePressure"), 1.0e-9d);
            assertEquals(10L, saturation.getLong("queuedTasks"));

            final RecordedEvent budget = single(events, "tuff.threads.BudgetChanged");
            assertEquals("compute", budget.getString("poolId"));
            assertEquals(-1, budget.getInt("previousCores"));
            assertEquals(2, budget.getInt("cores"));
            assertEquals(0, budget.getInt("previousReservedCores"));
            assertEquals(1, budget.getInt("reservedCores"));
            assertEquals(1.0d, budget.getDouble("previousShare"), 1.0e-9d);

            final RecordedEvent unknown = single(events, "tuff.threads.UnknownThreads");
            assertEquals(77L, unknown.getLong("threadId"));
            assertEquals("stray-worker", unknown.getString("threadName"));

            final RecordedEvent lease = single(events, "tuff.threads.LeaseTimeout");
            assertEquals("compute", lease.getString("poolId"));
            assertEquals(9_000L, lease.getLong("deadlineNanos"));
            assertEquals(Duration.ZERO.toNanos(), lease.getLong("maxWaitNanos"));
        }
    }

    @Test
    void disabledSinkIsSilentAndNeverThrows() {
        final JfrThreadSink sink = new JfrThreadSink();
        assertFalse(sink.enabled());
        assertDoesNotThrow(() -> {
            sink.poolState(pool());
            sink.threadCreated(new ThreadSnapshot(1L, "compute-1", PoolId.COMPUTE, Thread.State.NEW, true, 5, -1L));
            sink.threadTerminated(new ThreadSnapshot(2L, "compute-2", PoolId.COMPUTE, Thread.State.TERMINATED, true, 5,
                -1L));
            sink.saturation(SaturationReport.unknown(PoolId.COMPUTE));
            sink.budgetChanged(PoolId.COMPUTE, ResourceBudget.defaults(), ResourceBudget.defaults());
            sink.unregisteredThread(3L, "stray");
            sink.leaseTimedOut(LeaseRequest.immediate(PoolId.COMPUTE, 0L));
        });
    }

    private static PoolSnapshot pool() {
        final PoolSpec spec = new PoolSpec(
            PoolId.COMPUTE,
            PoolKind.COMPUTE,
            "compute",
            "test pool",
            2,
            -1,
            CoverageLevel.L2,
            ResourceBudget.defaults()
        );
        return new PoolSnapshot(
            spec,
            4,
            2,
            3,
            1,
            12L,
            100L,
            0L,
            0L,
            0L,
            0.25d,
            new SaturationReport(PoolId.COMPUTE, 0.75d, 0.12d, 12L, 0L, SaturationReport.Level.BUSY),
            List.of(DegradedReason.METRICS_UNAVAILABLE)
        );
    }

    private List<RecordedEvent> readEvents(final Recording recording, final String fileName) throws IOException {
        final Path destination = this.tempDir.resolve(fileName);
        recording.dump(destination);
        final List<RecordedEvent> events = new ArrayList<>();
        try (RecordingFile file = new RecordingFile(destination)) {
            while (file.hasMoreEvents()) {
                events.add(file.readEvent());
            }
        }
        return events;
    }

    private static RecordedEvent single(final List<RecordedEvent> events, final String name) {
        final RecordedEvent event = events.stream()
            .filter(candidate -> name.equals(candidate.getEventType().getName()))
            .findFirst()
            .orElseThrow();
        final long count = events.stream()
            .filter(candidate -> name.equals(candidate.getEventType().getName()))
            .count();
        assertEquals(1L, count, name);
        return event;
    }
}
