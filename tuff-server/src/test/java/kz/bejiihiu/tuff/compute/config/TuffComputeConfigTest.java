package kz.bejiihiu.tuff.compute.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TuffComputeConfigTest {

    @TempDir
    Path directory;

    @Test
    void missingFileIsCreatedFromTheTemplate() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        assertFalse(Files.exists(this.file()));

        final ConfigLoadResult result = config.load();

        assertTrue(result.createdFile());
        assertTrue(Files.exists(this.file()));
        assertTrue(result.problems().isEmpty(), () -> "unexpected problems " + result.problems());
        assertTrue(result.diff().isEmpty(), "the first read has nothing to compare against");
        assertEquals(Duration.ofSeconds(5), result.settings().shutdownGracePeriod());
        assertEquals(
            Set.of("chunk-generation", "pathfinding", "ai-computation"),
            result.settings().consumers().overrides().keySet()
        );

        final String written = Files.readString(this.file(), StandardCharsets.UTF_8);
        assertTrue(written.contains("async-compute:"), "file must contain the section");
        assertTrue(written.contains("Tuff Compute Worker"), "file must contain the shipped defaults");
        assertTrue(written.contains("Tuff - server configuration"), "file must start with a header");

        final ConfigLoadResult second = config.load();
        assertFalse(second.createdFile());
        assertTrue(second.diff().isEmpty());
        assertEquals(result.settings(), second.settings());
        assertEquals(result.settings(), config.settings());
    }

    @Test
    void templateSurvivesUtf8RoundTrip() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        config.load();

        final String created = Files.readString(this.file(), StandardCharsets.UTF_8);
        assertTrue(created.contains("Tuff - server configuration"), "created file must carry the banner comment");

        Files.writeString(this.file(), """
            # Конфигурация движка вычислений Tuff
            async-compute:
              workers:
                # имя воркеров видно в дампах потоков
                thread-name-prefix: "Вычислитель"
            """, StandardCharsets.UTF_8);

        final ConfigLoadResult result = config.reload();

        assertTrue(result.problems().isEmpty(), () -> "unexpected problems " + result.problems());
        assertEquals("Вычислитель", config.settings().workers().threadNamePrefix());
        final String afterReload = Files.readString(this.file(), StandardCharsets.UTF_8);
        assertTrue(afterReload.contains("Конфигурация движка"), () -> "file lost its cyrillic comment after reload: [" + afterReload + "]");
    }

    @Test
    void malformedFileKeepsThePreviousSnapshot() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), """
            async-compute:
              workers:
                threads: 4
            """, StandardCharsets.UTF_8);
        config.load();
        final ComputeSettings before = config.settings();
        assertEquals(4, before.workers().threads());

        Files.writeString(this.file(), "async-compute: [unclosed\n", StandardCharsets.UTF_8);
        final ConfigLoadResult result = config.reload();

        assertTrue(result.keptPreviousSettings());
        assertTrue(result.hasProblems());
        assertTrue(result.diff().isEmpty());
        assertEquals(before, config.settings());
        assertTrue(
            config.problems().stream().anyMatch(problem -> problem.detail().contains("not valid YAML")),
            () -> "expected a yaml problem, got " + config.problems()
        );
    }

    @Test
    void unreadableFileKeepsThePreviousSnapshot() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), "async-compute:\n  enabled: false\n", StandardCharsets.UTF_8);
        config.load();
        final ComputeSettings before = config.settings();

        // a directory where the file is expected cannot be read as a file
        Files.delete(this.file());
        Files.createDirectory(this.file());
        final ConfigLoadResult result = config.reload();

        assertTrue(result.keptPreviousSettings());
        assertTrue(result.hasProblems());
        assertEquals(before, config.settings());
    }

    @Test
    void reloadReportsWhatChanged() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), """
            async-compute:
              workers:
                threads: 4
            """, StandardCharsets.UTF_8);
        config.load();

        Files.writeString(this.file(), """
            async-compute:
              workers:
                threads: 6
              queue:
                capacity: 4096
            """, StandardCharsets.UTF_8);
        final ConfigLoadResult reloaded = config.reload();

        assertEquals(6, config.settings().workers().threads());
        assertEquals(4096, config.settings().queue().capacity());
        assertEquals(
            List.of("async-compute.queue.capacity", "async-compute.workers.threads"),
            reloaded.diff().changedPaths()
        );
        assertEquals(
            List.of("async-compute.queue.capacity", "async-compute.workers.threads"),
            reloaded.diff().restartRequiredPaths()
        );
        assertTrue(reloaded.diff().livePaths().isEmpty());
        assertEquals("async-compute.queue.capacity, async-compute.workers.threads", reloaded.diff().summary());
    }

    @Test
    void everythingTheEngineCannotAdoptLiveNeedsARestart() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), """
            async-compute:
              workers:
                threads: 4
              metrics:
                sample-interval-seconds: 1
              adaptive:
                min-samples: 20
              shutdown-grace-seconds: 5
            """, StandardCharsets.UTF_8);
        config.load();

        Files.writeString(this.file(), """
            async-compute:
              workers:
                threads: 6
                keep-alive-seconds: 10
              queue:
                capacity: 4096
                overflow-policy: drop-oldest
              metrics:
                sample-interval-seconds: 2
              adaptive:
                min-samples: 10
              shutdown-grace-seconds: 3
            """, StandardCharsets.UTF_8);
        final ConfigLoadResult reloaded = config.reload();

        assertEquals(List.of(
            "async-compute.adaptive.min-samples",
            "async-compute.metrics.sample-interval-seconds",
            "async-compute.queue.capacity",
            "async-compute.queue.overflow-policy",
            "async-compute.shutdown-grace-seconds",
            "async-compute.workers.keep-alive-seconds",
            "async-compute.workers.threads"
        ), reloaded.diff().changedPaths());
        assertEquals(List.of(
            "async-compute.adaptive.min-samples",
            "async-compute.metrics.sample-interval-seconds",
            "async-compute.queue.capacity",
            "async-compute.shutdown-grace-seconds",
            "async-compute.workers.keep-alive-seconds",
            "async-compute.workers.threads"
        ), reloaded.diff().restartRequiredPaths());
        assertEquals(List.of("async-compute.queue.overflow-policy"), reloaded.diff().livePaths());
    }

    @Test
    void consumerPoliciesAreTheOnlyLiveChange() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), """
            async-compute:
              consumers:
                chunk-generation:
                  max-concurrent: 0.50
            """, StandardCharsets.UTF_8);
        config.load();

        Files.writeString(this.file(), """
            async-compute:
              consumers:
                chunk-generation:
                  max-concurrent: 0.40
            """, StandardCharsets.UTF_8);
        final ConfigLoadResult reloaded = config.reload();

        assertEquals(
            List.of("async-compute.consumers.chunk-generation.max-concurrent"),
            reloaded.diff().changedPaths()
        );
        assertTrue(reloaded.diff().restartRequiredPaths().isEmpty());
        assertEquals(
            List.of("async-compute.consumers.chunk-generation.max-concurrent"),
            reloaded.diff().livePaths()
        );
    }

    @Test
    void schedulingChangesRequireARestart() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), "async-compute:\n  scheduling:\n    strategy: fifo\n", StandardCharsets.UTF_8);
        config.load();

        Files.writeString(this.file(), "async-compute:\n  scheduling:\n    strategy: priority\n", StandardCharsets.UTF_8);
        final ConfigLoadResult reloaded = config.reload();

        assertEquals(
            List.of("async-compute.scheduling.strategy"),
            reloaded.diff().restartRequiredPaths()
        );
        assertTrue(reloaded.diff().livePaths().isEmpty());
    }

    @Test
    void laneChangesRequireARestart() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), """
            async-compute:
              lanes:
                virtual:
                  max-concurrent: 64
                  spill-queue-depth: 64
            """, StandardCharsets.UTF_8);
        config.load();

        Files.writeString(this.file(), """
            async-compute:
              lanes:
                virtual:
                  max-concurrent: 8
                  spill-queue-depth: 0
            """, StandardCharsets.UTF_8);
        final ConfigLoadResult reloaded = config.reload();

        assertEquals(8, config.settings().lanes().virtual().maxConcurrent());
        assertEquals(0, config.settings().lanes().virtual().spillQueueDepth());
        assertEquals(
            List.of(
                "async-compute.lanes.virtual.max-concurrent",
                "async-compute.lanes.virtual.spill-queue-depth"
            ),
            reloaded.diff().restartRequiredPaths()
        );
        assertTrue(reloaded.diff().livePaths().isEmpty());
    }

    @Test
    void applyTargetSeesOnlyRealChanges() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), """
            async-compute:
              consumers:
                default:
                  max-concurrent: 2
            """, StandardCharsets.UTF_8);

        final List<ComputeSettings> applied = new ArrayList<>();
        final List<ConfigDiff> diffs = new ArrayList<>();
        config.setApplyTarget((previous, updated, diff) -> {
            applied.add(updated);
            diffs.add(diff);
        });

        config.load();
        assertTrue(applied.isEmpty(), "the first read has no engine to update yet");

        config.reload();
        assertTrue(applied.isEmpty(), "an unchanged file must not touch the engine");

        Files.writeString(this.file(), """
            async-compute:
              consumers:
                default:
                  max-concurrent: 3
            """, StandardCharsets.UTF_8);
        config.reload();

        assertEquals(1, applied.size());
        assertEquals(
            ConsumerPolicy.ConcurrencyLimit.absolute(3),
            applied.getFirst().consumers().defaultPolicy().concurrencyLimit()
        );
        assertEquals(List.of("async-compute.consumers.default.max-concurrent"), diffs.getFirst().livePaths());
    }

    @Test
    void failingApplyTargetStillKeepsTheNewSnapshot() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), "async-compute:\n  workers:\n    threads: 2\n", StandardCharsets.UTF_8);
        config.setApplyTarget((previous, updated, diff) -> {
            throw new IllegalStateException("engine is not ready");
        });
        config.load();

        Files.writeString(this.file(), "async-compute:\n  workers:\n    threads: 5\n", StandardCharsets.UTF_8);
        final ConfigLoadResult reloaded = config.reload();

        assertFalse(reloaded.keptPreviousSettings());
        assertEquals(5, config.settings().workers().threads());
    }

    @Test
    void applyTargetIsNotCalledForKeptPreviousSettings() throws IOException {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), "async-compute:\n  workers:\n    threads: 2\n", StandardCharsets.UTF_8);
        config.load();

        final List<String> calls = new ArrayList<>();
        config.setApplyTarget((previous, updated, diff) -> calls.add(diff.summary()));

        Files.writeString(this.file(), "async-compute: [nope\n", StandardCharsets.UTF_8);
        config.reload();

        assertTrue(calls.isEmpty());
    }

    @Test
    void concurrentReadersSeeConsistentSnapshots() throws Exception {
        final TuffComputeConfig config = TuffComputeConfig.inDirectory(this.directory);
        Files.writeString(this.file(), "async-compute:\n  workers:\n    threads: 1\n", StandardCharsets.UTF_8);
        config.load();

        final AtomicBoolean running = new AtomicBoolean(true);
        final ExecutorService pool = Executors.newFixedThreadPool(4);
        final List<Future<?>> readers = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                readers.add(pool.submit(() -> {
                    while (running.get()) {
                        final ComputeSettings settings = config.settings();
                        assertNotNull(settings);
                        assertTrue(settings.workers().keepAlive().toMillis() > 0L);
                        assertTrue(settings.queue().capacity() >= 1);
                        assertTrue(settings.workers().minThreads() >= 1);
                        assertNotNull(config.problems());
                        assertNotNull(config.lastDiff());
                    }
                    return null;
                }));
            }

            for (int i = 0; i < 25; i++) {
                Files.writeString(
                    this.file(),
                    "async-compute:\n  workers:\n    threads: " + (i + 1) + "\n",
                    StandardCharsets.UTF_8
                );
                final ConfigLoadResult result = config.reload();
                assertFalse(result.settings().workers().automaticThreadCount());
            }
        } finally {
            running.set(false);
            pool.shutdown();
            assertTrue(pool.awaitTermination(30L, TimeUnit.SECONDS), "readers must finish");
        }

        for (final Future<?> reader : readers) {
            reader.get();
        }
        assertEquals(25, config.settings().workers().threads());
    }

    private Path file() {
        return this.directory.resolve(TuffComputeConfig.FILE_NAME);
    }
}
