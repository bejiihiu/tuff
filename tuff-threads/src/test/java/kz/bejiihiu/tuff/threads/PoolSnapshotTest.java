package kz.bejiihiu.tuff.threads;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Locks the {@code -1 = unknown} rule of {@link PoolSnapshot}.
 *
 * <p>The sampler writes {@code -1} for every counter it has no source for. That is only honest while
 * the record accepts the sentinel, so this test pins both sides: unknown values construct, values below
 * the sentinel do not, and a measured zero stays a zero.
 */
class PoolSnapshotTest {

    private static final PoolId POOL = PoolId.COMPUTE;

    @Test
    void unknownSentinelIsAcceptedForEveryOptionalValue() {
        final PoolSnapshot snapshot = new PoolSnapshot(
            spec(),
            0,
            0,
            -1,
            -1,
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            -1.0d,
            SaturationReport.unknown(POOL),
            List.of()
        );

        assertEquals(-1, snapshot.activeThreads());
        assertEquals(-1, snapshot.idleThreads());
        assertEquals(-1L, snapshot.queuedTasks());
        assertEquals(-1L, snapshot.queueCapacity());
        assertEquals(-1L, snapshot.submittedTasks());
        assertEquals(-1L, snapshot.completedTasks());
        assertEquals(-1L, snapshot.rejectedTasks());
        assertEquals(-1.0d, snapshot.cpuLoad(), 0.0d);
        assertEquals(SaturationReport.Level.UNKNOWN, snapshot.saturation().level());
    }

    @Test
    void measuredZerosStayZeros() {
        final PoolSnapshot snapshot = new PoolSnapshot(
            spec(),
            0,
            0,
            0,
            0,
            0L,
            0L,
            0L,
            0L,
            0L,
            0.0d,
            SaturationReport.unknown(POOL),
            List.of()
        );

        assertEquals(0, snapshot.activeThreads());
        assertEquals(0L, snapshot.submittedTasks());
        assertEquals(0.0d, snapshot.cpuLoad(), 0.0d);
    }

    @Test
    void valuesBelowTheSentinelAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(-2, 0, 0L, 0L, 0L, 0L, 0.0d));
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(0, -2, 0L, 0L, 0L, 0L, 0.0d));
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(0, 0, -2L, 0L, 0L, 0L, 0.0d));
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(0, 0, 0L, -2L, 0L, 0L, 0.0d));
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(0, 0, 0L, 0L, -2L, 0L, 0.0d));
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(0, 0, 0L, 0L, 0L, -2L, 0.0d));
        assertThrows(IllegalArgumentException.class, () -> this.snapshot(0, 0, 0L, 0L, 0L, 0L, -1.5d));
    }

    private PoolSnapshot snapshot(
        final int active,
        final int idle,
        final long queued,
        final long capacity,
        final long submitted,
        final long rejected,
        final double cpuLoad
    ) {
        return new PoolSnapshot(
            spec(),
            0,
            0,
            active,
            idle,
            queued,
            capacity,
            submitted,
            submitted,
            rejected,
            cpuLoad,
            SaturationReport.unknown(POOL),
            List.of()
        );
    }

    private static PoolSpec spec() {
        return PoolSpec.observeOnly(POOL, PoolKind.COMPUTE, "test", "unit test pool");
    }
}
