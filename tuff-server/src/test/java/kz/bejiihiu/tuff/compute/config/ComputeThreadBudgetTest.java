package kz.bejiihiu.tuff.compute.config;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComputeThreadBudgetTest {

    @Test
    void subtractsTheThreadsTheServerAlreadyOwns() {
        // 32 cores * 0.8 = 25, minus 8 region, 8 chunk workers, 2 IO and 4 network reserve
        assertEquals(3, ComputeThreadBudget.resolve(32, 8, 8, 2));
    }

    @Test
    void neverGoesBelowOne() {
        assertEquals(1, ComputeThreadBudget.resolve(4, 4, 8, 1));
        assertEquals(1, ComputeThreadBudget.resolve(1, 0, 0, 0));
    }

    @Test
    void appliesTheSameRuleWithoutServerThreads() {
        assertEquals(2, ComputeThreadBudget.resolve(8));
        assertEquals(1, ComputeThreadBudget.resolve(4));
        assertEquals(12, ComputeThreadBudget.resolve(20));
    }

    @Test
    void rejectsImpossibleInput() {
        assertThrows(IllegalArgumentException.class, () -> ComputeThreadBudget.resolve(0));
        assertThrows(IllegalArgumentException.class, () -> ComputeThreadBudget.resolve(-1, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> ComputeThreadBudget.resolve(8, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> ComputeThreadBudget.resolve(8, 0, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> ComputeThreadBudget.resolve(8, 0, 0, -1));
    }

    @Test
    void automaticSizingUsesTheBudgetInsteadOfRawCores() {
        final ComputeSettings settings = TuffComputeConfig.defaultSettings();
        final int budget = ComputeThreadBudget.resolve(32, 8, 8, 2);

        // the record reserves one budget unit for the server and honours min-threads as a floor
        assertEquals(2, settings.workers().resolveThreadCount(budget));
        assertEquals(9, settings.workers().resolveThreadCount(10));
    }

    @Test
    void configuredBoundsStillClampTheBudget() {
        final ComputeSettings.Workers workers = new ComputeSettings.Workers(-1, 2, 4, Duration.ofSeconds(30), "test");

        assertEquals(4, workers.resolveThreadCount(32));
        assertEquals(2, workers.resolveThreadCount(3));
    }
}
