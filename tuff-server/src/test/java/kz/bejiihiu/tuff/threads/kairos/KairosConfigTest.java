package kz.bejiihiu.tuff.threads.kairos;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.config.ThreadsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The tick level keys of the Kairos block, read from a real file.
 *
 * <p>{@code config/tuff.yml} is read once at startup, long before the region scheduler starts, so the
 * values have to survive that window in the opt-in gate. This test writes the section, loads it with
 * the real reader and checks what the scheduler would find when its turn comes; bad input must be
 * reported and leave the stock behaviour in place rather than crash the start.
 */
@Timeout(120)
class KairosConfigTest {

    @AfterEach
    void forgetTheGateAnswer() {
        KairosGate.disarm();
    }

    @Test
    void theReaderPublishesEveryTickLevelKey(@TempDir final Path directory) throws IOException {
        final ThreadsConfig.ThreadsLoadResult result = load(directory, """
            thread-management:
              region-execution:
                scheduler: kairos
                kairos-unstable: true
                kairos:
                  catch-up:
                    cap-ticks: 4
                    budget-millis: 75
                  steal:
                    threshold-millis: 9
                  affinity:
                    local-queue-size: 3
                  lease:
                    timeout-seconds: 2
            """);

        assertTrue(result.problems().isEmpty(), () -> "unexpected problems: " + result.problems());
        assertEquals(new KairosKernel(4, 75, 3, 9, 2_000), KairosGate.kernel());
        assertTrue(KairosGate.kernel().catchUp());
    }

    @Test
    void anUnreadableValueKeepsTheStockBehaviourAndIsReported(@TempDir final Path directory) throws IOException {
        final ThreadsConfig.ThreadsLoadResult result = load(directory, """
            thread-management:
              region-execution:
                kairos:
                  catch-up:
                    cap-ticks: 0
                    budget-millis: not a number
            """);

        assertEquals(KairosKernel.defaults(), KairosGate.kernel(), "one bad number must not enable anything");
        assertFalse(result.problems().isEmpty(), "the operator has to be told");
        assertTrue(
            result.problems().stream().anyMatch(problem -> problem.toString().contains("budget-millis")),
            () -> "the unreadable key must be named: " + result.problems()
        );
    }

    @Test
    void anAbsentKairosBlockLeavesThePortsOwnDefaults(@TempDir final Path directory) throws IOException {
        final ThreadsConfig.ThreadsLoadResult result = load(directory, """
            thread-management:
              enabled: true
            """);

        assertEquals(KairosKernel.defaults(), KairosGate.kernel());
        assertTrue(KairosGate.kernel().catchUpBudgetMillis() > 0);
        assertFalse(KairosGate.kernel().affinity());
        assertEquals(KairosIdleWork.off(), KairosGate.idleWork(), "idle work must default to off");
        assertTrue(result.problems().isEmpty(), () -> "unexpected problems: " + result.problems());
    }

    @Test
    void theReaderPublishesTheIdleWorkBlock(@TempDir final Path directory) throws IOException {
        final ThreadsConfig.ThreadsLoadResult result = load(directory, """
            thread-management:
              region-execution:
                kairos:
                  idle-work:
                    enabled: true
                    mid-tick: true
                    utmi-jobs: true
                    slice-millis: 3
                    max-added-lag-millis: 9
                    max-share-percent: 40
            """);

        assertTrue(result.problems().isEmpty(), () -> "unexpected problems: " + result.problems());
        final KairosIdleWork idle = KairosGate.idleWork();
        assertEquals(new KairosIdleWork(true, true, true, 3, 9, 40), idle);
        assertTrue(idle.armed());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(3L), idle.sliceNanos());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(9L), idle.maxAddedLagNanos());
    }

    @Test
    void anUnreadableIdleWorkValueFallsBackToOff(@TempDir final Path directory) throws IOException {
        final ThreadsConfig.ThreadsLoadResult result = load(directory, """
            thread-management:
              region-execution:
                kairos:
                  idle-work:
                    enabled: true
                    mid-tick: true
                    slice-millis: 0
            """);

        assertEquals(KairosIdleWork.off(), KairosGate.idleWork(), "one bad number must not enable the path");
        assertTrue(
            result.problems().stream().anyMatch(problem -> problem.toString().contains("idle-work")),
            () -> "the unreadable key must be named: " + result.problems()
        );
    }

    private static ThreadsConfig.ThreadsLoadResult load(final Path directory, final String yaml) throws IOException {
        final Path file = directory.resolve("tuff.yml");
        Files.writeString(file, yaml, StandardCharsets.UTF_8);
        return new ThreadsConfig(file).load();
    }
}
