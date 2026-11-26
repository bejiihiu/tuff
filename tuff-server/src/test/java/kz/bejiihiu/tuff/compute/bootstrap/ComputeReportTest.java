package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.config.ConfigDiff;
import kz.bejiihiu.tuff.compute.config.ConfigLoadResult;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsReport;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(60)
class ComputeReportTest {

    private static final int QUEUE_CAPACITY = 128;

    @Test
    void aDisabledEngineGetsAnExplanationInsteadOfNumbers() {
        final List<String> lines = ComputeReport.overview(ComputeStatus.disabled());

        assertEquals(2, lines.size());
        assertTrue(lines.get(0).contains("disabled in config/tuff.yml"), lines.get(0));
        assertTrue(lines.get(0).contains("async-compute.enabled: false"), lines.get(0));
        assertTrue(lines.get(1).contains("restart"), lines.get(1));
    }

    @Test
    void aNotInitialisedEngineIsReportedAsSuch() {
        final List<String> lines = ComputeReport.overview(ComputeStatus.notInitialised());

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("has not been initialized"), lines.get(0));
    }

    @Test
    void aFailedEnginePointsAtTheLog() {
        final List<String> lines = ComputeReport.consumers(ComputeStatus.failed());

        assertEquals(2, lines.size());
        assertTrue(lines.get(0).contains("failed to start"), lines.get(0));
        assertTrue(lines.get(1).contains("Tuff/Compute"), lines.get(1));
    }

    @Test
    void theSummaryOfARunningEngineDescribesTheSnapshot() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        try {
            final int answer = scheduler.submit(() -> 41 + 1).get(10L, TimeUnit.SECONDS);
            assertEquals(42, answer);
            awaitCompleted(scheduler, 1L);

            final String report = String.join("\n", ComputeReport.overview(ComputeStatus.running(scheduler)));

            assertTrue(report.contains("Tuff compute: running"), report);
            assertTrue(report.contains("workers: "), report);
            assertTrue(report.contains("live ("), report);
            assertTrue(report.contains("concurrency limit "), report);
            assertTrue(report.contains("lanes: platform "), report);
            assertTrue(report.contains("virtual 0/64"), report);
            assertTrue(report.contains("queue: 0 / " + QUEUE_CAPACITY), report);
            assertTrue(report.contains("tasks: 1 submitted, 1 completed, 0 failed, 0 rejected, 0 cancelled, 0 expired"), report);
            assertTrue(report.contains("throughput: "), report);
            assertTrue(report.contains("queue wait: avg "), report);
            assertTrue(report.contains("execution: avg "), report);
            assertTrue(report.contains("cpu: "), report);
            assertTrue(report.contains("settings: workers requested 2, 2 live (min 1, max 2"), report);
            assertTrue(report.contains("queue " + QUEUE_CAPACITY + " REJECT"), report);
            assertTrue(report.contains("lane settings: virtual lane enabled, LOW on the virtual lane"), report);
        } finally {
            stopScheduler(scheduler);
        }
    }

    @Test
    void theConsumerBreakdownShowsPolicyAndCounters() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        try {
            scheduler.registerConsumer(new ComputeConsumer(
                "test-consumer",
                "Test Consumer",
                Set.of(ComputeTaskType.GENERAL),
                ComputePriority.NORMAL,
                ConsumerPolicy.absolute(1)
            ));
            final String result = scheduler
                .submit("test-consumer", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "done")
                .get(10L, TimeUnit.SECONDS);
            assertEquals("done", result);
            awaitCompleted(scheduler, 1L);

            final String report = String.join("\n", ComputeReport.consumers(ComputeStatus.running(scheduler)));

            assertTrue(report.contains("Tuff compute consumers ("), report);
            assertTrue(report.contains("test-consumer (Test Consumer): enabled, 1 concurrent"), report);
            assertTrue(report.contains("active 0 / limit 1, queued 0 / unbounded"), report);
            assertTrue(report.contains("1 submitted, 1 completed, 0 failed, 0 rejected, 0 cancelled, 0 expired"), report);
            assertTrue(report.contains("queue wait avg "), report);
        } finally {
            stopScheduler(scheduler);
        }
    }

    @Test
    void tasksOfAnIdleEngineSayNothingIsRunning() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        try {
            assertEquals(
                List.of("Tuff compute: nothing is running."),
                ComputeReport.tasks(ComputeStatus.running(scheduler), 10)
            );
        } finally {
            stopScheduler(scheduler);
        }
    }

    @Test
    void tasksOfADisabledEngineExplainWhyThereAreNone() {
        final List<String> lines = ComputeReport.tasks(ComputeStatus.disabled(), 10);

        assertEquals(2, lines.size());
        assertTrue(lines.get(0).contains("disabled in config/tuff.yml"), lines.get(0));
    }

    @Test
    void tasksListRunningWorkAndRecentCompletions() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final ComputeTaskHandle<String> handle = scheduler.enqueue(ComputeRequest.of("blocked", () -> {
                started.countDown();
                release.await();
                return "done";
            }));
            assertTrue(started.await(10L, TimeUnit.SECONDS), "the task should have started");

            final String runningReport = String.join(
                "\n", ComputeReport.tasks(ComputeStatus.running(scheduler), 10));

            assertTrue(runningReport.contains("Tuff compute tasks: 1 running"), runningReport);
            assertTrue(runningReport.contains(
                "#" + handle.taskId() + " platform default GENERAL NORMAL \"blocked\" RUNNING "), runningReport);

            release.countDown();
            assertEquals("done", handle.future().get(10L, TimeUnit.SECONDS));
            awaitCompleted(scheduler, 1L);
            awaitRecentTasks(scheduler, 1);

            final String finishedReport = String.join(
                "\n", ComputeReport.tasks(ComputeStatus.running(scheduler), 10));

            assertTrue(finishedReport.contains("0 running"), finishedReport);
            assertTrue(finishedReport.contains(" COMPLETED "), finishedReport);
            assertTrue(finishedReport.contains(" / wait "), finishedReport);
            assertTrue(finishedReport.contains("\"blocked\""), finishedReport);
        } finally {
            release.countDown();
            stopScheduler(scheduler);
        }
    }

    @Test
    void theTaskCountCapsOnlyTheRecentSection() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        try {
            for (int index = 0; index < 3; index++) {
                scheduler.submit(() -> "value").get(10L, TimeUnit.SECONDS);
            }
            awaitCompleted(scheduler, 3L);
            awaitRecentTasks(scheduler, 3);

            final String report = String.join(
                "\n", ComputeReport.tasks(ComputeStatus.running(scheduler), 1));

            assertTrue(report.contains("slowest 1 recent"), report);
            assertEquals(1, occurrences(report, " COMPLETED "), report);
        } finally {
            stopScheduler(scheduler);
        }
    }

    @Test
    void aReloadReportSeparatesAppliedFromDeferredChanges() {
        final ConfigDiff diff = new ConfigDiff(
            List.of(
                "async-compute.consumers.default.fair-share-weight",
                "async-compute.queue.capacity",
                "async-compute.workers.threads"
            ),
            List.of("async-compute.queue.capacity")
        );
        final ConfigLoadResult result = new ConfigLoadResult(
            ComputeSettings.defaults(),
            List.of(new ConfigProblem("async-compute.workers.threads", "must be -1 or at least 1, got 0 (using default -1)")),
            diff,
            false,
            false
        );
        final ReloadReport report = new ReloadReport(
            result,
            List.of("default"),
            List.of(),
            List.of("async-compute.queue.capacity", "async-compute.workers.threads")
        );

        final String text = String.join("\n", ComputeReport.reload(report));

        assertTrue(text.contains("3 change(s)"), text);
        assertTrue(text.contains("applied now: consumer policies for default"), text);
        assertTrue(text.contains("takes effect after a restart: async-compute.queue.capacity, async-compute.workers.threads"), text);
        assertTrue(text.contains("problems (1):"), text);
        assertTrue(text.contains("async-compute.workers.threads: must be -1"), text);
    }

    @Test
    void restartOnlyChangesAreNotReportedAsApplied() {
        final List<String> restartKeys = List.of(
            "async-compute.adaptive.enabled",
            "async-compute.metrics.enabled",
            "async-compute.scheduling.strategy",
            "async-compute.shutdown-grace-seconds",
            "async-compute.workers.threads"
        );
        final ConfigDiff diff = new ConfigDiff(restartKeys, restartKeys);
        final ConfigLoadResult result = new ConfigLoadResult(ComputeSettings.defaults(), List.of(), diff, false, false);
        final ReloadReport report = new ReloadReport(
            result,
            List.of(),
            List.of(),
            ConsumerReload.deferredPaths(diff, List.of(), List.of())
        );

        final String text = String.join("\n", ComputeReport.reload(report));

        assertTrue(text.contains("takes effect after a restart: " + String.join(", ", restartKeys)), text);
        assertFalse(text.contains("applied now"), text);
        assertTrue(report.deferredPaths().containsAll(restartKeys), report.deferredPaths().toString());
    }

    @Test
    void aReloadWithoutChangesSaysSo() {
        final ConfigLoadResult result = new ConfigLoadResult(
            ComputeSettings.defaults(), List.of(), ConfigDiff.empty(), false, false);
        final List<String> lines = ComputeReport.reload(new ReloadReport(result, List.of(), List.of(), List.of()));

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("nothing changed"), lines.get(0));
    }

    @Test
    void everyCommandSurfaceSurvivesBothSubsystemsBeingDisabled() {
        // /tuff and /tuff compute with the engine off: the summary, consumer and task branches all
        // fall back to an explanation instead of dereferencing a missing scheduler
        assertFalse(ComputeReport.overview(ComputeStatus.disabled()).isEmpty());
        assertFalse(ComputeReport.consumers(ComputeStatus.disabled()).isEmpty());
        assertFalse(ComputeReport.tasks(ComputeStatus.disabled(), 5).isEmpty());

        // /tuff threads with UTMI disabled, and with a bootstrap that was never started
        assertFalse(ThreadsReport.overview(
            ThreadsStatus.disabled(ThreadManagementSettings.defaults())).isEmpty());
        assertFalse(ThreadsReport.overview(ThreadsStatus.notInitialised()).isEmpty());
    }

    @Test
    void aReloadThatCouldNotReadTheFileKeepsThePreviousSettings() {
        final ConfigLoadResult result = new ConfigLoadResult(
            ComputeSettings.defaults(), List.of(), ConfigDiff.empty(), false, true);
        final List<String> lines = ComputeReport.reload(new ReloadReport(result, List.of(), List.of(), List.of()));

        assertTrue(lines.get(0).contains("could not be read"), lines.get(0));
        assertTrue(lines.get(0).contains("previous settings stay in effect"), lines.get(0));
    }

    @Test
    void aReloadWithoutConsumerChangesHasNoAppliedLine() {
        final ConfigDiff diff = new ConfigDiff(
            List.of("async-compute.queue.capacity"), List.of("async-compute.queue.capacity"));
        final ConfigLoadResult result = new ConfigLoadResult(ComputeSettings.defaults(), List.of(), diff, false, false);
        final String report = String.join("\n", ComputeReport.reload(
            new ReloadReport(result, List.of(), List.of(), List.of("async-compute.queue.capacity"))));

        assertFalse(report.contains("applied now"), report);
    }

    @Test
    void aReloadForAnUnregisteredConsumerIsReportedAsSavedNotApplied() {
        final ConfigDiff diff = new ConfigDiff(
            List.of("async-compute.consumers.ghost.max-concurrent"), List.of());
        final ConfigLoadResult result = new ConfigLoadResult(ComputeSettings.defaults(), List.of(), diff, false, false);

        final String report = String.join("\n", ComputeReport.reload(
            new ReloadReport(result, List.of(), List.of("ghost"), List.of())));

        assertFalse(report.contains("applied now"), report);
        assertTrue(report.contains("saved for a consumer id that is not registered yet: ghost"), report);
        assertFalse(report.contains("takes effect after a restart"), report);
    }

    @Test
    void laneLinesSeparateThePlatformPoolFromTheVirtualLane() {
        assertEquals(
            "  lanes: platform 4/5 (3 busy, 1 idle) | virtual 12/64 (9 queued, 3 spilled)",
            ComputeReport.describeLanes(
                4,
                3,
                1,
                5,
                new SchedulerMetrics.VirtualLaneMetrics(true, 12, 64, 9, 20L, 8L, 0L, 1L, 3L)
            )
        );
    }

    @Test
    void laneLinesHandleAnIdleEngineAndADisabledLane() {
        assertEquals(
            "  lanes: platform 0/1 (0 busy, 0 idle) | virtual 0/64 (0 queued, 0 spilled)",
            ComputeReport.describeLanes(
                0,
                0,
                0,
                1,
                new SchedulerMetrics.VirtualLaneMetrics(true, 0, 64, 0, 0L, 0L, 0L, 0L, 0L)
            )
        );
        assertEquals(
            "  lanes: platform 2/2 (0 busy, 2 idle) | virtual disabled (LOW tasks use the platform workers)",
            ComputeReport.describeLanes(2, 0, 2, 2, SchedulerMetrics.VirtualLaneMetrics.disabled())
        );
    }

    @Test
    void laneSettingsShowThresholdsAndTheDisabledDepthTrigger() {
        assertEquals(
            "  lane settings: virtual lane enabled, LOW on the virtual lane, NORMAL spills when saturated, "
                + "max-concurrent 64, spill after 50 ms or 64 queued NORMAL tasks",
            ComputeReport.describeLaneSettings(ComputeSettings.Lanes.Virtual.defaults())
        );
        assertEquals(
            "  lane settings: virtual lane enabled, LOW on the platform workers, NORMAL never spills, "
                + "max-concurrent 8, spill after 0 ms (queue depth trigger off)",
            ComputeReport.describeLaneSettings(
                new ComputeSettings.Lanes.Virtual(true, false, false, 8, Duration.ZERO, 0)
            )
        );
        assertEquals(
            "  lane settings: virtual lane disabled, LOW tasks run on the platform workers",
            ComputeReport.describeLaneSettings(ComputeSettings.Lanes.Virtual.withoutVirtual())
        );
    }

    @Test
    void laneChangesAreReportedAsNeedingARestart() {
        final ConfigDiff diff = new ConfigDiff(
            List.of("async-compute.lanes.virtual.max-concurrent"),
            List.of("async-compute.lanes.virtual.max-concurrent")
        );
        final ConfigLoadResult result = new ConfigLoadResult(ComputeSettings.defaults(), List.of(), diff, false, false);
        final String report = String.join("\n", ComputeReport.reload(
            new ReloadReport(result, List.of(), List.of(), List.of("async-compute.lanes.virtual.max-concurrent"))));

        assertTrue(report.contains("takes effect after a restart: async-compute.lanes.virtual.max-concurrent"), report);
        assertFalse(report.contains("applied now"), report);
    }

    @Test
    void durationsAreFormattedWithAFixedPrecision() {
        assertEquals("0.00 ms", ComputeReport.formatDuration(Duration.ZERO));
        assertEquals("0.85 ms", ComputeReport.formatDuration(Duration.ofNanos(850_000L)));
        assertEquals("12.0 ms", ComputeReport.formatDuration(Duration.ofMillis(12L)));
        assertEquals("999.9 ms", ComputeReport.formatDuration(Duration.ofMillis(999L).plusNanos(900_000L)));
        assertEquals("1.50 s", ComputeReport.formatDuration(Duration.ofMillis(1500L)));
        assertEquals("61.25 s", ComputeReport.formatDuration(Duration.ofSeconds(61L, 250_000_000L)));
    }

    @Test
    void ratesAndCpuHideMissingValues() {
        assertEquals("12.3 tasks/s", ComputeReport.formatRate(12.34d));
        assertEquals("0.0 tasks/s", ComputeReport.formatRate(0.0d));
        assertEquals("34.1% of the process", ComputeReport.formatCpu(0.3412d));
        assertEquals("n/a (not sampled)", ComputeReport.formatCpu(Double.NaN));
    }

    @Test
    void recordingFileNamesCarryAUtcTimestamp() {
        assertEquals(
            "tuff-compute-2026-11-26T14-05-12-345Z.jfr",
            ComputeJfr.fileName(Instant.parse("2026-11-26T14:05:12.345Z"))
        );
    }

    @Test
    void statusCarriesASchedulerOnlyWhenItIsRunning() {
        final AsyncComputeScheduler scheduler = ComputeSchedulers.create(ComputeSettings.defaults());
        try {
            assertEquals(AsyncComputeScheduler.SchedulerState.NEW, scheduler.state());
            final ComputeStatus running = ComputeStatus.running(scheduler);
            assertTrue(running.isRunning());
            assertEquals(scheduler, running.engine().orElseThrow());
            assertFalse(ComputeStatus.disabled().isRunning());
            assertTrue(ComputeStatus.disabled().engine().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> new ComputeStatus(ComputeStatus.State.RUNNING, null));
            assertThrows(IllegalArgumentException.class, () -> new ComputeStatus(ComputeStatus.State.DISABLED, scheduler));
        } finally {
            scheduler.shutdown(Duration.ZERO);
        }
    }

    @Test
    void aStartedSchedulerTerminatesCleanly() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        stopScheduler(scheduler);

        assertEquals(AsyncComputeScheduler.SchedulerState.TERMINATED, scheduler.state());
    }

    private static AsyncComputeScheduler startScheduler() {
        final ComputeSettings defaults = ComputeSettings.defaults();
        final ComputeSettings settings = new ComputeSettings(
            true,
            new ComputeSettings.Workers(2, 1, 2, Duration.ofSeconds(5L), "Tuff Compute Test"),
            new ComputeSettings.QueueSettings(QUEUE_CAPACITY, OverflowPolicy.REJECT),
            defaults.scheduling(),
            defaults.consumers(),
            defaults.metrics(),
            defaults.adaptive(),
            Duration.ofSeconds(1L)
        );
        final AsyncComputeScheduler scheduler = ComputeSchedulers.create(settings, ComputeClock.system(), 2);
        scheduler.start();
        return scheduler;
    }

    private static void stopScheduler(final AsyncComputeScheduler scheduler) throws InterruptedException {
        scheduler.shutdown(Duration.ZERO);
        assertTrue(scheduler.awaitTermination(Duration.ofSeconds(10L)));
    }

    private static void awaitRecentTasks(final AsyncComputeScheduler scheduler, final int expected)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (scheduler.metrics().slowestRecentTasks(50).size() < expected) {
            if (System.nanoTime() > deadline) {
                fail("the scheduler retained " + scheduler.metrics().slowestRecentTasks(50).size()
                    + " recent task(s), expected " + expected);
            }
            Thread.sleep(1L);
        }
    }

    private static int occurrences(final String text, final String needle) {
        int count = 0;
        int index = text.indexOf(needle);
        while (index >= 0) {
            count++;
            index = text.indexOf(needle, index + needle.length());
        }
        return count;
    }

    private static void awaitCompleted(final AsyncComputeScheduler scheduler, final long expected)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (scheduler.metrics().completedTasks() < expected) {
            if (System.nanoTime() > deadline) {
                fail("the scheduler reported " + scheduler.metrics().completedTasks() + " completed task(s), expected " + expected);
            }
            Thread.sleep(1L);
        }
    }
}
