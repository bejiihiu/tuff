package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerSizingTest {

    @Test
    void automaticTargetLeavesRoomForTickThreadsAndJvmWork() {
        assertEquals(11, WorkerSizing.automaticTarget(16));
        assertEquals(23, WorkerSizing.automaticTarget(32));
        assertEquals(47, WorkerSizing.automaticTarget(64));
        assertEquals(4, WorkerSizing.automaticTarget(8));
    }

    @Test
    void automaticTargetIsClampedToTheConfiguredWindow() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(2, 6, Duration.ofSeconds(30), "x");
        final int target = WorkerSizing.targetWorkers(workers, 16);
        assertTrue(target <= 6, "target must respect maxThreads, got " + target);
        assertTrue(target >= 2, "target must respect minThreads, got " + target);
    }

    @Test
    void fixedSizingUsesTheConfiguredThreadCount() {
        final ComputeSettings.Workers workers = PoolTestSupport.fixed(3, 1, 8, "x");
        assertEquals(3, WorkerSizing.targetWorkers(workers, 16));
        assertEquals(8, WorkerSizing.maxWorkers(workers, 16));
    }

    @Test
    void fixedSizingHonoursAnExplicitCeilingAboveTheProcessorCount() {
        final ComputeSettings.Workers workers = PoolTestSupport.fixed(6, 1, 8, "x");
        assertEquals(6, WorkerSizing.targetWorkers(workers, 4));
        assertEquals(8, WorkerSizing.maxWorkers(workers, 4));
    }

    @Test
    void maximumNeverDropsBelowTheMinimum() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(4, -1, Duration.ofSeconds(30), "x");
        assertEquals(4, WorkerSizing.maxWorkers(workers, 2));
    }
}
