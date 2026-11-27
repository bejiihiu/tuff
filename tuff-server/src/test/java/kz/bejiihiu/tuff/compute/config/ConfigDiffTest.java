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
    void reliefKeysAreRestartOnly() {
        final ConfigDiff diff = ConfigDiff.between(
            Map.of("async-compute.relief.enabled", "false"),
            Map.of(
                "async-compute.relief.enabled", "true",
                "async-compute.relief.reserved-cores", "2",
                "async-compute.relief.consumer.max-concurrent", "0.5"
            )
        );

        assertEquals(3, diff.changedPaths().size(), diff.changedPaths().toString());
        assertEquals(diff.changedPaths(), diff.restartRequiredPaths());
        assertTrue(diff.livePaths().isEmpty(), "relief is wired into the pool at start, so no key is live");
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
