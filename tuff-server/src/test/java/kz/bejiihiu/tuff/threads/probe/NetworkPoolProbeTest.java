package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetworkPoolProbeTest {

    @Test
    void observesTheCanonicalNetworkPool() {
        assertEquals(PoolId.NETWORK, new NetworkPoolProbe().pool());
    }

    @Test
    void matchesPlayerAndManagementLoopsByName() {
        final NetworkPoolProbe probe = new NetworkPoolProbe(() -> List.of(
            thread("Netty NIO IO #0", Thread.State.RUNNABLE),
            thread("Netty Epoll IO #1", Thread.State.WAITING),
            thread("Netty Kqueue IO #2", Thread.State.TIMED_WAITING),
            thread("Management server IO #0", Thread.State.RUNNABLE),
            thread("Netty NIO #3", Thread.State.RUNNABLE),
            thread("Folia Region Scheduler Thread #0", Thread.State.RUNNABLE),
            thread("Async Chat Thread - #0", Thread.State.RUNNABLE)
        ));

        final PoolObservation observation = probe.observe();

        assertEquals(4, observation.aliveThreads(), "player loops and the management server are netty");
        assertEquals(2, observation.activeThreads(), "RUNNABLE counts as active");
        assertEquals(2, observation.idleThreads(), "WAITING and TIMED_WAITING count as idle");
        assertEquals(4, observation.threads().size());
        assertEquals(0, observation.coreThreads(), "an event loop has no UTMI floor");
        assertEquals(-1L, observation.queuedTasks(), "netty does not expose a queue depth");
        assertEquals(-1L, observation.queueCapacity());
    }

    @Test
    void noLoopsYetIsAMeasuredZero() {
        final PoolObservation observation = new NetworkPoolProbe(List::of).observe();

        assertEquals(0, observation.aliveThreads(), "lazily created loops mean zero, not unknown");
        assertEquals(0, observation.activeThreads());
        assertEquals(0, observation.idleThreads());
        assertTrue(observation.threads().isEmpty());
    }

    @Test
    void aFailingThreadTableReportsUnknownActivity() {
        final PoolObservation observation = new NetworkPoolProbe(() -> {
            throw new IllegalStateException("the thread table went away");
        }).observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(-1, observation.activeThreads(), "an unreadable source is unknown, not idle");
        assertEquals(-1, observation.idleThreads());
        assertEquals(-1L, observation.queuedTasks());
        assertTrue(observation.threads().isEmpty());
    }

    @Test
    void theNameMatcherIsNotAFreeForAll() {
        assertTrue(NetworkPoolProbe.isEventLoop("Netty NIO IO #0"));
        assertTrue(NetworkPoolProbe.isEventLoop("Management server IO #3"));
        assertFalse(NetworkPoolProbe.isEventLoop("Netty NIO #0"));
        assertFalse(NetworkPoolProbe.isEventLoop("Netty NIO IO"));
        assertFalse(NetworkPoolProbe.isEventLoop(null));
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
