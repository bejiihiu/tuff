package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
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
        lines.add(INDENT + "settings: workers " + describeWorkers(settings.workers())
            + ", queue " + settings.queue().capacity() + " " + settings.queue().overflowPolicy()
            + ", scheduling " + settings.scheduling().strategy());
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
        return List.copyOf(lines);
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

    private static String describeWorkers(final ComputeSettings.Workers workers) {
        final String count = workers.automaticThreadCount() ? "auto" : Integer.toString(workers.threads());
        final String maximum = workers.maxThreads() == -1 ? "auto" : Integer.toString(workers.maxThreads());
        return count + " (min " + workers.minThreads() + ", max " + maximum
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
