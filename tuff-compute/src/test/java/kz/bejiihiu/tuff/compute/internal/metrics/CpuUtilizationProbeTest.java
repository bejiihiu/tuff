package kz.bejiihiu.tuff.compute.internal.metrics;

import java.util.OptionalDouble;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.CpuUtilizationProbe;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CpuUtilizationProbeTest {

    @Test
    void reportsOnlyRealLoadOrNothing() {
        final OptionalDouble sample = CpuUtilizationProbe.sample();

        if (!CpuUtilizationProbe.supported()) {
            assertTrue(sample.isEmpty());
            return;
        }
        assertTrue(
            sample.isEmpty() || (sample.getAsDouble() >= 0.0d && sample.getAsDouble() <= 1.0d),
            "cpu load must be empty or inside [0, 1]"
        );
    }
}
