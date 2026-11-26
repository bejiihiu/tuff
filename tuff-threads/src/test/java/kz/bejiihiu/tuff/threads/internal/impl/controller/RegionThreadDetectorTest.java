package kz.bejiihiu.tuff.threads.internal.impl.controller;

import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.impl.registry.ThreadRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.testing.RecordingEventSink;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the order of evidence for "is this a tick thread": registry first, name only for strangers.
 */
class RegionThreadDetectorTest {

    private final RecordingEventSink sink = new RecordingEventSink();
    private final ThreadRegistryImpl registry = new ThreadRegistryImpl(this.sink);

    @Test
    void theRegistryDecidesForRegisteredThreadsWhateverTheirName() {
        final RegionThreadDetector detector =
            RegionThreadDetector.registered(this.registry, RegionThreadDetector.byThreadName());

        final Thread compute = new Thread(() -> { }, "Folia Region Scheduler Thread #4");
        this.registry.register(compute, PoolId.COMPUTE);
        assertFalse(detector.isRegionThread(compute), "a registered compute thread is not a tick thread");

        final Thread tick = new Thread(() -> { }, "renamed-tick-thread");
        this.registry.register(tick, PoolId.REGION_EXECUTION);
        assertTrue(detector.isRegionThread(tick), "a renamed tick thread is still a tick thread");
    }

    @Test
    void theNameIsOnlyAFallbackForUnregisteredThreads() {
        final RegionThreadDetector detector =
            RegionThreadDetector.registered(this.registry, RegionThreadDetector.byThreadName());

        assertTrue(detector.isRegionThread(new Thread(() -> { }, "Folia Region Scheduler Thread #0")));
        assertFalse(detector.isRegionThread(new Thread(() -> { }, "ordinary-worker")));
    }
}
