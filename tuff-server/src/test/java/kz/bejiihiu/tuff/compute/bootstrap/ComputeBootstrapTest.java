package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputeBootstrapTest {

    private static final Duration KEEP_ALIVE = Duration.ofSeconds(30L);

    @Test
    void aRequestAboveTheBudgetIsReportedAsClamped() {
        // max-threads: -1 means the processor budget is the ceiling
        assertTrue(ComputeBootstrap.budgetClampsRequestedThreads(workers(6, 2, -1), 4));
        assertFalse(ComputeBootstrap.budgetClampsRequestedThreads(workers(4, 2, -1), 4));
        assertFalse(ComputeBootstrap.budgetClampsRequestedThreads(workers(2, 2, -1), 4));
    }

    @Test
    void automaticSizingAndAnExplicitMaximumAreNotReportedAsClamped() {
        assertFalse(ComputeBootstrap.budgetClampsRequestedThreads(workers(-1, 2, -1), 1));
        // an explicit max-threads wins over the budget, so the requested count is what runs
        assertFalse(ComputeBootstrap.budgetClampsRequestedThreads(workers(6, 2, 8), 4));
    }

    @Test
    void theAwaitAfterShutdownStaysInsideTheRestartBudget() {
        // a long grace is capped, so a hung task cannot eat the /restart budget
        assertEquals(Duration.ofSeconds(5L), ComputeBootstrap.shutdownTimeout(Duration.ofSeconds(30L)));
        assertEquals(Duration.ofSeconds(5L), ComputeBootstrap.shutdownTimeout(Duration.ofSeconds(5L)));
        assertEquals(Duration.ofSeconds(5L), ComputeBootstrap.shutdownTimeout(Duration.ofSeconds(4L)));
        assertEquals(Duration.ofSeconds(2L), ComputeBootstrap.shutdownTimeout(Duration.ofSeconds(1L)));
        assertEquals(Duration.ofSeconds(1L), ComputeBootstrap.shutdownTimeout(Duration.ZERO));
    }

    private static ComputeSettings.Workers workers(final int threads, final int minThreads, final int maxThreads) {
        return new ComputeSettings.Workers(threads, minThreads, maxThreads, KEEP_ALIVE, "Tuff Compute Test");
    }
}
