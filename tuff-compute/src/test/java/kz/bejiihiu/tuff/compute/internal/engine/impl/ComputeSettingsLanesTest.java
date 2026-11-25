package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lane configuration model validates itself before the engine sees it.
 */
class ComputeSettingsLanesTest {

    @Test
    void defaultsEnableLowOwnershipAndSpill() {
        final ComputeSettings.Lanes.Virtual virtual = ComputeSettings.Lanes.defaults().virtual();
        assertTrue(virtual.enabled());
        assertTrue(virtual.lowPriority());
        assertTrue(virtual.servesLow());
        assertTrue(virtual.normalSpill());
        assertTrue(virtual.allowsSpill());
        assertTrue(virtual.maxConcurrent() >= 1);
        assertTrue(virtual.spillQueueDepth() >= 0);
    }

    @Test
    void withoutVirtualFallsBackToThePlatformLane() {
        final ComputeSettings.Lanes.Virtual virtual = ComputeSettings.Lanes.withoutVirtual().virtual();
        assertFalse(virtual.servesLow());
        assertFalse(virtual.allowsSpill());
    }

    @Test
    void invalidVirtualOptionsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ComputeSettings.Lanes.Virtual(
            true, true, true, 0, Duration.ZERO, 0));
        assertThrows(IllegalArgumentException.class, () -> new ComputeSettings.Lanes.Virtual(
            true, true, true, 1, Duration.ofMillis(-1L), 0));
        assertThrows(IllegalArgumentException.class, () -> new ComputeSettings.Lanes.Virtual(
            true, true, true, 1, Duration.ZERO, -1));
    }

    @Test
    void legacySettingsConstructorUsesDefaultLanes() {
        final ComputeSettings defaults = ComputeSettings.defaults();
        final ComputeSettings legacy = new ComputeSettings(
            defaults.enabled(),
            defaults.workers(),
            defaults.queue(),
            defaults.scheduling(),
            defaults.consumers(),
            defaults.metrics(),
            defaults.adaptive(),
            defaults.shutdownGracePeriod()
        );
        assertEquals(ComputeSettings.Lanes.defaults(), legacy.lanes());
    }
}
