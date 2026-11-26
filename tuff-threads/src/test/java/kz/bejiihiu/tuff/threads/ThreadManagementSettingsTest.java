package kz.bejiihiu.tuff.threads;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the safe defaults and the validation invariants phase two builds on.
 */
class ThreadManagementSettingsTest {

    @Test
    void defaultsAreObserveOnly() {
        final ThreadManagementSettings settings = ThreadManagementSettings.defaults();

        assertTrue(settings.enabled());
        assertFalse(settings.anyManagementEnabled());
        assertFalse(settings.regionExecution().managed());
        assertEquals(ThreadManagementSettings.RegionScheduler.EDF, settings.regionExecution().scheduler());
        assertFalse(settings.regionExecution().kairosUnstable());
        assertEquals(1, settings.resources().reserveCores());
        assertTrue(settings.executionPools().chunkSystem().preserveSpecialization());
        assertTrue(settings.metrics().enabled());
    }

    @Test
    void kairosRequiresExplicitOptIn() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ThreadManagementSettings.RegionExecution(
                true, true, ThreadManagementSettings.RegionScheduler.KAIROS, -1, 4, false
            )
        );
    }

    @Test
    void foliaSchedulingCannotBeDisabled() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ThreadManagementSettings.RegionExecution(
                true, false, ThreadManagementSettings.RegionScheduler.EDF, -1, 4, false
            )
        );
    }

    @Test
    void poolSpecializationCannotBeDisabled() {
        assertThrows(IllegalArgumentException.class, () -> new ThreadManagementSettings.PoolSettings(true, false));
    }

    @Test
    void managedPoolNeedsAThreadFloor() {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.CHUNK_WORKER, PoolKind.CHUNK, "Moonrise", "chunk generation"
        );

        assertEquals(CoverageLevel.L1, spec.coverage());
        assertThrows(IllegalArgumentException.class, () -> spec.withCoverage(CoverageLevel.L2));
    }

    @Test
    void elasticBudgetResolvesAgainstUsableCores() {
        assertEquals(2, ResourceBudget.elastic(0.5d, 0).resolveCores(4));
        assertEquals(1, ResourceBudget.elastic(0.1d, 0).resolveCores(4));
        assertEquals(3, ResourceBudget.fixed(3, 1).resolveCores(8));
        assertEquals(2, ResourceBudget.fixed(3, 1).resolveCores(2));
    }
}
