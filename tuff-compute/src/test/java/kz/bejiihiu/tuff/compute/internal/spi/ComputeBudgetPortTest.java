package kz.bejiihiu.tuff.compute.internal.spi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The default budget port must report the same number the no-argument factory used, and a fixed port
 * must hand back exactly what the caller resolved.
 */
class ComputeBudgetPortTest {

    @Test
    void legacyUsesTheJvmProcessorCount() {
        final ComputeBudgetPort legacy = ComputeBudgetPort.legacy();
        assertEquals(Runtime.getRuntime().availableProcessors(), legacy.processors());
        assertEquals("legacy", legacy.source());
    }

    @Test
    void fixedKeepsTheResolvedNumberAndLabel() {
        final ComputeBudgetPort fixed = ComputeBudgetPort.fixed(3, "utmi");
        assertEquals(3, fixed.processors());
        assertEquals("utmi", fixed.source());
    }

    @Test
    void fixedRejectsAnImpossibleBudget() {
        assertThrows(IllegalArgumentException.class, () -> ComputeBudgetPort.fixed(0, "tuff.yml"));
        assertThrows(IllegalArgumentException.class, () -> ComputeBudgetPort.fixed(-2, "tuff.yml"));
        assertThrows(IllegalArgumentException.class, () -> ComputeBudgetPort.fixed(2, " "));
    }
}
