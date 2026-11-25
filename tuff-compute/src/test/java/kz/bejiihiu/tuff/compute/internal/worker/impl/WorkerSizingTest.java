package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Sizing is a pure delegation to the worker contract, with the processor budget as input.
 */
class WorkerSizingTest {

    @Test
    void automaticSizingUsesTheContractFormula() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(1, -1, Duration.ofSeconds(30), "x");
        assertEquals(15, WorkerSizing.targetWorkers(workers, 16));
        assertEquals(16, WorkerSizing.maxWorkers(workers, 16));
        assertEquals(1, WorkerSizing.targetWorkers(workers, 1));
        assertEquals(1, WorkerSizing.maxWorkers(workers, 1));
        assertEquals(31, WorkerSizing.targetWorkers(workers, 32));
        assertEquals(32, WorkerSizing.maxWorkers(workers, 32));
    }

    @Test
    void aBudgetOfOneIsStillAOneWorkerPool() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(1, -1, Duration.ofSeconds(30), "x");
        assertEquals(1, WorkerSizing.targetWorkers(workers, 1));
        assertEquals(1, WorkerSizing.maxWorkers(workers, 1));
    }

    @Test
    void minimumWinsWhenItExceedsTheBudget() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(4, -1, Duration.ofSeconds(30), "x");
        assertEquals(4, WorkerSizing.targetWorkers(workers, 2));
        assertEquals(4, WorkerSizing.maxWorkers(workers, 2));
    }

    @Test
    void maximumBelowMinimumIsRejectedByTheContract() {
        assertThrows(IllegalArgumentException.class, () -> new ComputeSettings.Workers(
            -1,
            4,
            2,
            Duration.ofSeconds(30),
            "x"
        ));
    }

    @Test
    void fixedSizingUsesTheConfiguredThreadCountAndExplicitCeiling() {
        final ComputeSettings.Workers workers = PoolTestSupport.fixed(3, 1, 8, "x");
        assertEquals(3, WorkerSizing.targetWorkers(workers, 16));
        assertEquals(8, WorkerSizing.maxWorkers(workers, 16));
        assertEquals(3, WorkerSizing.targetWorkers(workers, 1));
        assertEquals(8, WorkerSizing.maxWorkers(workers, 1));
    }

    @Test
    void automaticMaximumFollowsTheBudget() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(2, -1, Duration.ofSeconds(30), "x");
        assertEquals(6, WorkerSizing.targetWorkers(workers, 7));
        assertEquals(7, WorkerSizing.maxWorkers(workers, 7));
        assertEquals(2, WorkerSizing.targetWorkers(workers, 2));
        assertEquals(2, WorkerSizing.maxWorkers(workers, 2));
    }

    @Test
    void nonPositiveBudgetIsRejected() {
        final ComputeSettings.Workers workers = PoolTestSupport.automatic(1, -1, Duration.ofSeconds(30), "x");
        assertThrows(IllegalArgumentException.class, () -> WorkerSizing.targetWorkers(workers, 0));
        assertThrows(IllegalArgumentException.class, () -> WorkerSizing.maxWorkers(workers, 0));
    }
}
