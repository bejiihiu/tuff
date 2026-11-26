package kz.bejiihiu.tuff.compute.config;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigDiffTest {

    @Test
    void theWholeThreadManagementSectionIsRestartOnly() {
        final ConfigDiff diff = ConfigDiff.between(
            Map.of(),
            Map.of(
                "thread-management.enabled", "false",
                "thread-management.region-execution.threads", "8",
                "thread-management.metrics.per-thread", "false"
            )
        );

        assertEquals(
            List.of(
                "thread-management.enabled",
                "thread-management.metrics.per-thread",
                "thread-management.region-execution.threads"
            ),
            diff.changedPaths()
        );
        assertEquals(diff.changedPaths(), diff.restartRequiredPaths());
        assertTrue(diff.livePaths().isEmpty(), "no thread-management key may be applied to a running server");
    }

    @Test
    void computeConsumersStayTheOnlyLiveKeys() {
        final ConfigDiff diff = ConfigDiff.between(
            Map.of("async-compute.consumers.default.max-queued-tasks", "-1"),
            Map.of("async-compute.consumers.default.max-queued-tasks", "64")
        );

        assertTrue(diff.restartRequiredPaths().isEmpty());
        assertEquals(List.of("async-compute.consumers.default.max-queued-tasks"), diff.livePaths());
    }
}
