package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.config.ComputeThreadBudget;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
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

    @Test
    void theStartupBudgetHasExactlyOneSource() {
        final CountingBudget managed = new CountingBudget(3);

        assertEquals(3, ComputeBootstrap.startupBudget(managed, 64));
        assertEquals(1, managed.calls(), "the managed port is the only source that was asked");
        // the legacy formula is a different number here, so the assertion above is not a coincidence
        assertEquals(ComputeThreadBudget.resolve(64), ComputeBootstrap.startupBudget(null, 64));
        assertTrue(ComputeThreadBudget.resolve(64) > 3);
    }

    @Test
    void aManagedBudgetIsNotSubtractedTwice() {
        final CountingBudget quiet = new CountingBudget(7);
        final CountingBudget busy = new CountingBudget(7);

        // the live thread counts must not be taken off a number that already excludes them
        assertEquals(7, ComputeBootstrap.liveBudget(quiet, 64, 0, 0, 0));
        assertEquals(7, ComputeBootstrap.liveBudget(busy, 64, 4, 6, 2));
        assertEquals(1, quiet.calls());
        assertEquals(1, busy.calls());

        // the legacy branch keeps doing exactly what it did before
        assertEquals(
            ComputeThreadBudget.resolve(64, 4, 6, 2),
            ComputeBootstrap.liveBudget(null, 64, 4, 6, 2)
        );
        assertTrue(ComputeThreadBudget.resolve(64, 4, 6, 2) < ComputeThreadBudget.resolve(64));
    }

    private static ComputeSettings.Workers workers(final int threads, final int minThreads, final int maxThreads) {
        return new ComputeSettings.Workers(threads, minThreads, maxThreads, KEEP_ALIVE, "Tuff Compute Test");
    }

    private static final class CountingBudget implements ComputeBudgetPort {

        private final int processors;
        private int calls;

        CountingBudget(final int processors) {
            this.processors = processors;
        }

        @Override
        public int processors() {
            this.calls++;
            return this.processors;
        }

        @Override
        public String source() {
            return "test";
        }

        int calls() {
            return this.calls;
        }
    }
}
