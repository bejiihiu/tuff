package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.RegionReliefMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.config.ConfigLoadResult;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import org.jspecify.annotations.Nullable;

/**
 * Turns engine state and snapshots into the lines {@code /tuff} prints.
 *
 * <p>Everything here is plain text and free of server state, so the whole command surface can be
 * tested without a running server. The command decorates the lines with colour; colours and layout
 * stay in one place instead of leaking into the metrics code.
 *
 * <p>Numbers come from the public {@link SchedulerMetrics} snapshot only. That snapshot exposes
 * averages and longest values, not percentiles, so the report shows averages and the long tail rather
 * than inventing a distribution it cannot see.
 */
public final class ComputeReport {

    private static final String INDENT = "  ";

    private ComputeReport() {
    }

    /**
     * Describes why no engine data is available.
     *
     * @param status current bootstrap status
     * @return one line explaining what an administrator should check
     * @throws IllegalArgumentException if the engine is running and therefore has data
     */
    public static List<String> unavailable(final ComputeStatus status) {
        return switch (status.state()) {
            case NOT_INITIALISED -> List.of("The Tuff compute engine has not been initialized yet.");
            case DISABLED -> List.of(
                "The Tuff compute engine is disabled in config/tuff.yml (async-compute.enabled: false).",
                INDENT + "Set it to true and restart the server to enable the shared worker pool."
            );
            case FAILED -> List.of(
                "The Tuff compute engine failed to start; the server is running without it.",
                INDENT + "The cause is in the server log, look for the \"Tuff/Compute\" logger."
            );
            case RUNNING -> throw new IllegalArgumentException("a running compute engine has data to report");
        };
    }

    /**
     * Builds the summary shown by {@code /tuff} and {@code /tuff compute}.
     *
     * @param status current bootstrap status
     * @return the report lines, or the reason there is nothing to report
     */
    public static List<String> overview(final ComputeStatus status) {
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return unavailable(status);
        }
        final SchedulerMetrics metrics = scheduler.metrics();
        final ComputeSettings settings = scheduler.settings();
        final List<String> lines = new ArrayList<>();
        lines.add("Tuff compute: " + describeState(scheduler.state())
            + (metrics.enabled() ? "" : " (metrics collection is disabled, counters stay at zero)"));
        lines.add(INDENT + "workers: " + metrics.totalWorkers() + " live (" + metrics.activeWorkers() + " busy, "
            + metrics.idleWorkers() + " idle), concurrency limit " + metrics.concurrencyLimit()
            + ", peak " + metrics.peakWorkers());
        lines.add(describeLanes(
            metrics.totalWorkers(),
            metrics.activeWorkers(),
            metrics.idleWorkers(),
            metrics.concurrencyLimit(),
            metrics.lanes().virtual()
        ));
        lines.add(INDENT + "queue: " + metrics.queuedTasks() + " / " + metrics.queueCapacity());
        lines.add(INDENT + "tasks: " + metrics.submittedTasks() + " submitted, " + metrics.completedTasks()
            + " completed, " + metrics.failedTasks() + " failed, " + metrics.rejectedTasks() + " rejected, "
            + metrics.cancelledTasks() + " cancelled, " + metrics.expiredTasks() + " expired");
        lines.add(INDENT + "throughput: " + formatRate(metrics.throughputPerSecond()));
        lines.add(INDENT + "queue wait: avg " + formatDuration(metrics.averageQueueWait())
            + ", longest " + formatDuration(metrics.longestQueueWait()));
        lines.add(INDENT + "execution: avg " + formatDuration(metrics.averageExecutionTime())
            + ", longest " + formatDuration(metrics.longestExecutionTime()));
        lines.add(INDENT + "cpu: " + formatCpu(metrics.cpuUtilization()));
        lines.add(INDENT + "settings: workers " + describeWorkers(settings.workers(), metrics.totalWorkers())
            + ", queue " + settings.queue().capacity() + " " + settings.queue().overflowPolicy()
            + ", scheduling " + settings.scheduling().strategy());
        lines.add(describeLaneSettings(settings.lanes().virtual()));
        return List.copyOf(lines);
    }

    /**
     * Builds the per consumer breakdown shown by {@code /tuff compute consumers}.
     *
     * @param status current bootstrap status
     * @return the report lines, or the reason there is nothing to report
     */
    public static List<String> consumers(final ComputeStatus status) {
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return unavailable(status);
        }
        final SchedulerMetrics metrics = scheduler.metrics();
        final Set<String> ids = new TreeSet<>(scheduler.consumerIds());
        ids.addAll(metrics.consumers().keySet());
        if (ids.isEmpty()) {
            return List.of("Tuff compute has no registered consumers.");
        }

        final List<String> lines = new ArrayList<>();
        lines.add("Tuff compute consumers (" + ids.size() + "):");
        for (final String id : ids) {
            final @Nullable ComputeConsumer consumer = scheduler.consumer(id).orElse(null);
            final @Nullable ConsumerMetrics consumerMetrics = metrics.consumer(id).orElse(null);
            final String name = consumer != null && !consumer.displayName().equals(id)
                ? id + " (" + consumer.displayName() + ")"
                : id;
            lines.add(INDENT + name + ": " + describePolicy(consumer));
            if (consumerMetrics == null) {
                lines.add(INDENT + INDENT + "no metrics recorded yet");
                continue;
            }
            lines.add(INDENT + INDENT + "active " + consumerMetrics.activeTasks()
                + " / limit " + consumerMetrics.concurrencyLimit()
                + ", queued " + consumerMetrics.queuedTasks() + " / " + describeQueueBound(consumerMetrics.maxQueuedTasks()));
            lines.add(INDENT + INDENT + consumerMetrics.submittedTasks() + " submitted, "
                + consumerMetrics.completedTasks() + " completed, " + consumerMetrics.failedTasks() + " failed, "
                + consumerMetrics.rejectedTasks() + " rejected, " + consumerMetrics.cancelledTasks() + " cancelled, "
                + consumerMetrics.expiredTasks() + " expired, " + formatRate(consumerMetrics.throughputPerSecond()));
            lines.add(INDENT + INDENT + "queue wait avg " + formatDuration(consumerMetrics.averageQueueWait())
                + " / longest " + formatDuration(consumerMetrics.longestQueueWait())
                + ", execution avg " + formatDuration(consumerMetrics.averageExecutionTime())
                + " / longest " + formatDuration(consumerMetrics.longestExecutionTime()));
        }
        metrics.regionRelief().ifPresent(relief -> lines.addAll(describeRelief(relief)));
        return List.copyOf(lines);
    }

    /**
     * Builds the region relief lines shown below the consumer table.
     *
     * <p>Relief has counters the generic consumer snapshot cannot carry (stale results, inline
     * fallbacks, estimated saved time), so they are printed as one block. A disabled relief path is
     * still printed: an operator has to be able to see that the path exists and is switched off.
     *
     * @param relief relief snapshot
     * @return the lines of the relief block
     */
    static List<String> describeRelief(final RegionReliefMetrics relief) {
        return List.of(
            INDENT + "region relief: " + (relief.enabled() ? "enabled" : "off, every request runs inline")
                + ", " + relief.inFlight() + " in flight / " + relief.maxInFlightPerRegion() + " per region"
                + ", reserve " + relief.reservedCores() + " core(s)",
            INDENT + INDENT + relief.requests() + " requests, " + relief.accepted() + " accepted, "
                + relief.completed() + " completed, " + relief.stale() + " stale, "
                + relief.inlineFallbacks() + " inline fallbacks (" + relief.expired() + " expired), "
                + relief.failed() + " failed, " + relief.dropped() + " dropped, "
                + formatDuration(Duration.ofNanos(relief.estimatedSavedNanos())) + " estimated saved"
        );
    }

    /**
     * Builds the task listing shown by {@code /tuff compute tasks}.
     *
     * <p>The running section is sorted by how long each body has been executing, the recent
     * section comes from the completion ring already sorted by execution time. Both views are read
     * from one metrics snapshot, so the numbers match the state the command observed.
     *
     * @param status current bootstrap status
     * @param limit  maximum number of recent completions to show
     * @return the report lines, or the reason there is nothing to report
     */
    public static List<String> tasks(final ComputeStatus status, final int limit) {
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler == null) {
            return unavailable(status);
        }
        final SchedulerMetrics metrics = scheduler.metrics();
        final List<SchedulerMetrics.RunningTaskInfo> running = new ArrayList<>(metrics.runningTasks());
        running.sort((first, second) -> Long.compare(second.elapsedNanos(), first.elapsedNanos()));
        final List<SchedulerMetrics.CompletedTaskInfo> recent = metrics.slowestRecentTasks(limit);
        if (running.isEmpty() && recent.isEmpty()) {
            return List.of("Tuff compute: nothing is running.");
        }

        final List<String> lines = new ArrayList<>();
        lines.add("Tuff compute tasks: " + running.size() + " running, slowest " + recent.size() + " recent");
        for (final SchedulerMetrics.RunningTaskInfo task : running) {
            lines.add(INDENT + describeTaskPrefix(
                task.taskId(),
                task.lane(),
                task.consumerId(),
                task.taskType().name(),
                task.priority().name(),
                task.label()
            ) + " RUNNING " + formatDuration(Duration.ofNanos(task.elapsedNanos())));
        }
        for (final SchedulerMetrics.CompletedTaskInfo task : recent) {
            lines.add(INDENT + describeTaskPrefix(
                task.taskId(),
                task.lane(),
                task.consumerId(),
                task.taskType().name(),
                task.priority().name(),
                task.label()
            ) + ' ' + task.state().name() + ' ' + formatDuration(Duration.ofNanos(task.executionNanos()))
                + " / wait " + formatDuration(Duration.ofNanos(task.queueWaitNanos())));
        }
        return List.copyOf(lines);
    }

    /**
     * Formats the columns every task row shares.
     *
     * @param taskId     unique task id
     * @param lane       execution lane
     * @param consumerId consumer the task belongs to
     * @param taskType   declared task category
     * @param priority   submitted priority
     * @param label      optional diagnostic label
     * @return the shared row prefix, without the trailing state and timing
     */
    static String describeTaskPrefix(
        final long taskId,
        final String lane,
        final String consumerId,
        final String taskType,
        final String priority,
        final Optional<String> label
    ) {
        return "#" + taskId + ' ' + lane + ' ' + consumerId + ' ' + taskType + ' ' + priority + ' ' + describeLabel(label);
    }

    /**
     * Renders a label without letting odd input break the one line row.
     *
     * @param label optional label
     * @return the quoted label, or {@code -} when the submission had none
     */
    private static String describeLabel(final Optional<String> label) {
        if (label.isEmpty()) {
            return "-";
        }
        final String safe = label.get().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
        return '"' + safe.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    /**
     * Builds the output of {@code /tuff compute reload}.
     *
     * @param report what the reload read, applied and deferred
     * @return the report lines
     */
    public static List<String> reload(final ReloadReport report) {
        final ConfigLoadResult result = report.result();
        final List<String> lines = new ArrayList<>();
        if (result.keptPreviousSettings()) {
            lines.add("The compute configuration could not be read; the previous settings stay in effect.");
        } else if (result.diff().isEmpty()) {
            lines.add("The compute configuration was reloaded, nothing changed.");
        } else {
            lines.add("The compute configuration was reloaded, " + result.diff().changedPaths().size() + " change(s).");
            if (!report.appliedConsumerIds().isEmpty()) {
                lines.add(INDENT + "applied now: consumer policies for " + String.join(", ", report.appliedConsumerIds()));
            }
            if (!report.deferredConsumerIds().isEmpty()) {
                lines.add(INDENT + "saved for a consumer id that is not registered yet: "
                    + String.join(", ", report.deferredConsumerIds())
                    + " (the policy applies when that consumer registers)");
            }
            if (!report.deferredPaths().isEmpty()) {
                lines.add(INDENT + "takes effect after a restart: " + String.join(", ", report.deferredPaths()));
            }
        }
        if (result.hasProblems()) {
            lines.add(INDENT + "problems (" + result.problems().size() + "):");
            for (final ConfigProblem problem : result.problems()) {
                lines.add(INDENT + INDENT + problem);
            }
        }
        return List.copyOf(lines);
    }

    /**
     * Formats a duration for the command output.
     *
     * @param duration duration to format, never {@code null}
     * @return the value in milliseconds or seconds with a fixed precision
     */
    static String formatDuration(final Duration duration) {
        if (duration.getSeconds() >= 1L) {
            return String.format(Locale.ROOT, "%.2f s", duration.getSeconds() + duration.getNano() / 1_000_000_000.0d);
        }
        final double millis = duration.getNano() / 1_000_000.0d;
        if (millis < 1.0d) {
            return String.format(Locale.ROOT, "%.2f ms", millis);
        }
        return String.format(Locale.ROOT, "%.1f ms", millis);
    }

    /**
     * Formats a task rate for the command output.
     *
     * @param rate tasks per second
     * @return the rate with one decimal place
     */
    static String formatRate(final double rate) {
        return String.format(Locale.ROOT, "%.1f tasks/s", rate);
    }

    /**
     * Formats the process wide CPU estimate for the command output.
     *
     * @param utilization value between {@code 0.0} and {@code 1.0}, or {@code NaN} when unavailable
     * @return the percentage, or a reason it is missing
     */
    static String formatCpu(final double utilization) {
        if (Double.isNaN(utilization)) {
            return "n/a (not sampled)";
        }
        return String.format(Locale.ROOT, "%.1f%% of the process", utilization * 100.0d);
    }

    private static String describeState(final AsyncComputeScheduler.SchedulerState state) {
        return state.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    /**
     * Builds the lane line: the platform pool on the left, the virtual lane on the right.
     *
     * <p>The platform pair is live workers over the current concurrency limit, which adaptive
     * concurrency may keep below the pool size. Virtual numbers come from
     * {@link SchedulerMetrics.VirtualLane}: {@code running} and {@code limit} are gauges,
     * {@code queued} is the lane backlog (LOW entries plus spill eligible NORMAL entries) and
     * {@code spilled} is the cumulative number of NORMAL tasks the lane executed because the
     * platform pool stayed saturated and a spill threshold fired. It is not a current gauge, and a
     * NORMAL task never leaves the platform lane in advance: it stays in the shared queue and any
     * platform worker that gets to it first runs it normally.
     *
     * @param liveWorkers      worker threads that exist
     * @param busyWorkers      workers executing a task
     * @param idleWorkers      workers waiting for a task
     * @param concurrencyLimit current scheduler concurrency limit
     * @param virtual          virtual lane snapshot
     * @return the formatted line
     */
    static String describeLanes(
        final int liveWorkers,
        final int busyWorkers,
        final int idleWorkers,
        final int concurrencyLimit,
        final SchedulerMetrics.VirtualLane virtual
    ) {
        final String platform = "lanes: platform " + liveWorkers + "/" + concurrencyLimit
            + " (" + busyWorkers + " busy, " + idleWorkers + " idle)";
        if (!virtual.enabled()) {
            return INDENT + platform + " | virtual disabled (LOW tasks use the platform workers)";
        }
        return INDENT + platform + " | virtual " + virtual.running() + "/" + virtual.limit()
            + " (" + virtual.queued() + " queued, " + virtual.spilled() + " spilled)";
    }

    /**
     * Builds the line with the effective virtual lane settings.
     *
     * <p>This is the configuration the running engine was created with, not what the last reload
     * read: lane settings need a restart before they change anything.
     *
     * @param virtual configured virtual lane
     * @return the formatted line
     */
    static String describeLaneSettings(final ComputeSettings.Lanes.Virtual virtual) {
        if (!virtual.enabled()) {
            return INDENT + "lane settings: virtual lane disabled, LOW tasks run on the platform workers";
        }
        final StringBuilder out = new StringBuilder(INDENT).append("lane settings: virtual lane enabled, ");
        out.append(virtual.lowPriority() ? "LOW on the virtual lane" : "LOW on the platform workers");
        out.append(", ").append(virtual.normalSpill() ? "NORMAL spills when saturated" : "NORMAL never spills");
        out.append(", max-concurrent ").append(virtual.maxConcurrent());
        out.append(", spill after ").append(virtual.spillWait().toMillis()).append(" ms");
        if (virtual.spillQueueDepth() == 0) {
            out.append(" (queue depth trigger off)");
        } else {
            out.append(" or ").append(virtual.spillQueueDepth()).append(" queued NORMAL tasks");
        }
        return out.toString();
    }

    private static String describeWorkers(final ComputeSettings.Workers workers, final int liveWorkers) {
        final String requested = workers.automaticThreadCount() ? "auto" : Integer.toString(workers.threads());
        final String maximum = workers.maxThreads() == -1 ? "auto" : Integer.toString(workers.maxThreads());
        return "requested " + requested + ", " + liveWorkers + " live (min " + workers.minThreads() + ", max " + maximum
            + ", keep-alive " + workers.keepAlive().toSeconds() + "s)";
    }

    private static String describeQueueBound(final int maxQueuedTasks) {
        return maxQueuedTasks == -1 ? "unbounded" : Integer.toString(maxQueuedTasks);
    }

    private static String describePolicy(final @Nullable ComputeConsumer consumer) {
        if (consumer == null) {
            return "registered by the engine, policy not tracked";
        }
        final ConsumerPolicy policy = consumer.policy();
        final List<String> parts = new ArrayList<>();
        parts.add(policy.enabled() ? "enabled" : "disabled");
        parts.add(describeLimit(policy.concurrencyLimit()));
        parts.add("weight " + String.format(Locale.ROOT, "%.2f", policy.fairShareWeight()));
        parts.add("max queued " + describeQueueBound(policy.maxQueuedTasks()));
        parts.add("overflow " + policy.overflowPolicy());
        parts.add("max wait " + (policy.maxQueueWait().isZero() ? "none" : formatDuration(policy.maxQueueWait())));
        return String.join(", ", parts);
    }

    private static String describeLimit(final ConsumerPolicy.ConcurrencyLimit limit) {
        return switch (limit) {
            case final ConsumerPolicy.AbsoluteLimit absolute -> absolute.permits() + " concurrent";
            case final ConsumerPolicy.RelativeLimit relative -> Math.round(relative.fraction() * 100.0d) + "% of the pool";
        };
    }
}
