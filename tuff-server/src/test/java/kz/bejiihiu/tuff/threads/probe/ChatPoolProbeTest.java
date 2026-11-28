package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class ChatPoolProbeTest {

    @Test
    void observesTheCanonicalChatPool() {
        assertEquals(PoolId.CHAT, new ChatPoolProbe().pool());
    }

    @Test
    void usesTheExecutorCountersWhenTheyAreAvailable() {
        final ChatPoolProbe probe = new ChatPoolProbe(
            () -> new PoolCounters(3, 0, 2, 0L),
            () -> List.of(
                thread("Async Chat Thread - #0", Thread.State.RUNNABLE),
                thread("Async Chat Thread - #1", Thread.State.RUNNABLE),
                thread("Async Chat Thread - #2", Thread.State.WAITING),
                thread("Async Chat Thread - #3", Thread.State.WAITING)
            )
        );

        final PoolObservation observation = probe.observe();

        assertEquals(4, observation.aliveThreads(), "alive is the larger of counters and the name scan");
        assertEquals(0, observation.coreThreads(), "a cached executor has no core threads");
        assertEquals(2, observation.activeThreads());
        assertEquals(2, observation.idleThreads());
        assertEquals(0L, observation.queuedTasks(), "the cached executor hands tasks straight to threads");
        assertEquals(-1L, observation.queueCapacity(), "an unbounded handoff has no capacity to report");
        assertEquals(4, observation.threads().size());
    }

    @Test
    void fallsBackToNamesWhenTheExecutorIsNotThere() {
        final ChatPoolProbe probe = new ChatPoolProbe(
            () -> null,
            () -> List.of(
                thread("Async Chat Thread - #0", Thread.State.RUNNABLE),
                thread("Async Chat Thread - #1", Thread.State.TIMED_WAITING),
                thread("Some Other Thread", Thread.State.RUNNABLE)
            )
        );

        final PoolObservation observation = probe.observe();

        assertEquals(2, observation.aliveThreads());
        assertEquals(1, observation.activeThreads());
        assertEquals(1, observation.idleThreads());
        assertEquals(-1L, observation.queuedTasks(), "no executor means no queue depth");
        assertEquals(2, observation.threads().size());
    }

    @Test
    void aBrokenCounterSourceFallsBackInsteadOfFailing() {
        final ChatPoolProbe probe = new ChatPoolProbe(
            () -> {
                throw new IllegalStateException("the executor went away");
            },
            () -> List.of(thread("Async Chat Thread - #0", Thread.State.WAITING))
        );

        final PoolObservation observation = probe.observe();

        assertEquals(1, observation.aliveThreads());
        assertEquals(1, observation.idleThreads());
    }

    @Test
    void nothingReadableReportsUnknownActivity() {
        final ChatPoolProbe probe = new ChatPoolProbe(() -> null, () -> {
            throw new IllegalStateException("the thread table went away");
        });

        final PoolObservation observation = probe.observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(-1, observation.activeThreads());
        assertEquals(-1, observation.idleThreads());
        assertTrue(observation.threads().isEmpty());
    }

    private static Thread thread(final String name, final Thread.State state) {
        return new Thread(name) {
            @Override
            public Thread.State getState() {
                return state;
            }
        };
    }
}
