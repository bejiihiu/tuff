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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the experimental opt in is a real gate, not a comment.
 *
 * <p>Two layers are checked: the runtime gate (a scheduler without the flag refuses to start and
 * leaves no thread behind) and the source of the flag ({@code kairos-unstable} in
 * {@code config/tuff.yml}), including the case where the file selects Kairos but forgets the flag.
 */
@Timeout(120)
class KairosGateTest {

    @TempDir
    Path directory;

    @AfterEach
    void disarm() {
        KairosGate.disarm();
    }

    @Test
    void kairosWithoutTheFlagRefusesToStart() {
        KairosGate.disarm();
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final KairosScheduler scheduler = new KairosScheduler(2, threads, tick -> 0L);

        final IllegalStateException refused = assertThrows(IllegalStateException.class, scheduler::start);

        assertTrue(refused.getMessage().contains("kairos-unstable"), refused.getMessage());
        assertTrue(refused.getMessage().contains("config/tuff.yml"), refused.getMessage());
        assertEquals(0, scheduler.getAliveThreads().length);
        assertFalse(threads.anyAlive(), "a refused start must not start a thread");
    }

    @Test
    void kairosWithTheFlagStarts() throws InterruptedException {
        KairosGate.arm(true);
        final RecordingThreadFactory threads = new RecordingThreadFactory();
        final KairosScheduler scheduler = new KairosScheduler(1, threads, tick -> 0L);
        try {
            scheduler.start();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
            while (scheduler.getAliveThreads().length != 1) {
                if (System.nanoTime() - deadline >= 0L) {
                    throw new AssertionError("the fund did not start its thread");
                }
                Thread.sleep(1L);
            }
        } finally {
            scheduler.halt();
            assertTrue(scheduler.join(5_000L));
        }
        assertFalse(threads.anyAlive());
    }

    @Test
    void theFlagIsReadFromTuffYml() throws IOException {
        final Path file = this.directory.resolve("tuff.yml");

        Files.writeString(file, """
            thread-management:
              region-execution:
                scheduler: kairos
                kairos-unstable: true
            """, StandardCharsets.UTF_8);
        final ThreadsConfig.ThreadsLoadResult armed = new ThreadsConfig(file).load();
        assertTrue(armed.problems().isEmpty(), () -> "unexpected problems: " + armed.problems());
        assertTrue(armed.settings().regionExecution().kairosUnstable());

        KairosGate.arm(armed.settings().regionExecution().kairosUnstable());
        assertTrue(KairosGate.armed());
        KairosGate.requireOptIn();

        Files.writeString(file, """
            thread-management:
              region-execution:
                scheduler: kairos
            """, StandardCharsets.UTF_8);
        final ThreadsConfig.ThreadsLoadResult missing = new ThreadsConfig(file).load();
        assertFalse(missing.problems().isEmpty(), "kairos without the flag must be reported");
        assertFalse(missing.settings().regionExecution().kairosUnstable());

        KairosGate.arm(missing.settings().regionExecution().kairosUnstable());
        assertThrows(IllegalStateException.class, KairosGate::requireOptIn);
    }
}
