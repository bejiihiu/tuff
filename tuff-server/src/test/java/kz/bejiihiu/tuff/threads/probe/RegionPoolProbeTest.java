package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegionPoolProbeTest {

    @Test
    void observesTheCanonicalRegionPool() {
        assertEquals(PoolId.REGION_EXECUTION, new RegionPoolProbe().pool());
    }

    @Test
    void classifiesThreadsByTheirState() {
        final RegionThreadScan scan = new RegionThreadScan(
            () -> List.of(
                threadIn(Thread.State.RUNNABLE),
                threadIn(Thread.State.WAITING),
                threadIn(Thread.State.TIMED_WAITING),
                threadIn(Thread.State.BLOCKED)
            ),
            () -> 4
        );

        final PoolObservation observation = new RegionPoolProbe(scan).observe();

        assertEquals(4, observation.aliveThreads());
        assertEquals(1, observation.activeThreads());
        assertEquals(2, observation.idleThreads(), "WAITING and TIMED_WAITING are idle; BLOCKED is neither");
        assertEquals(4, observation.threads().size());
    }

    @Test
    void theSchedulerCountWinsOverAShorterScan() {
        final RegionThreadScan scan = new RegionThreadScan(
            () -> List.of(threadIn(Thread.State.RUNNABLE)),
            () -> 8
        );

        final PoolObservation observation = new RegionPoolProbe(scan).observe();

        assertEquals(8, observation.aliveThreads(), "the scheduler count is authoritative for alive");
        assertEquals(1, observation.threads().size());
    }

    @Test
    void queueDepthAndCoreCountAreNotInvented() {
        final RegionThreadScan scan = new RegionThreadScan(
            () -> List.of(threadIn(Thread.State.RUNNABLE)),
            () -> 1
        );

        final PoolObservation observation = new RegionPoolProbe(scan).observe();

        assertEquals(0, observation.coreThreads(), "an observe-only pool claims no guaranteed floor");
        assertEquals(-1L, observation.queuedTasks(), "the queue depth needs a getter Folia does not expose");
        assertEquals(-1L, observation.queueCapacity());
    }

    @Test
    void aFailingSourceReportsZerosInsteadOfThrowing() {
        final RegionThreadScan scan = new RegionThreadScan(
            () -> {
                throw new IllegalStateException("the scheduler went away");
            },
            () -> {
                throw new IllegalStateException("the scheduler went away");
            }
        );

        final PoolObservation observation = new RegionPoolProbe(scan).observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(0, observation.activeThreads());
        assertEquals(0, observation.idleThreads());
        assertTrue(observation.threads().isEmpty());
    }

    /**
     * A thread whose state is fixed for the test. Real threads would have to be scheduled and parked,
     * which makes state assertions intermittent for no benefit.
     *
     * @param state the state the thread reports
     * @return an unstarted thread reporting that state
     */
    private static Thread threadIn(final Thread.State state) {
        return new Thread() {
            @Override
            public Thread.State getState() {
                return state;
            }
        };
    }
}
