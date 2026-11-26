package kz.bejiihiu.tuff.threads.bootstrap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.jspecify.annotations.Nullable;

/**
 * Turns the thread layer's status into the lines {@code /tuff threads} prints.
 *
 * <p>Plain text and free of server state, so every branch - disabled, failed, running, no snapshot yet -
 * can be tested without a server. The command adds colour; this class owns the wording.
 *
 * <p>Coverage is printed as the registered integration level, not as a promise: {@code L1} means observed
 * only, and the summary's degraded reasons say why a pool is not managed. Queue depth is left out on
 * purpose - observe-only pools report {@code -1} until their owner exposes it, and a printed "0" would
 * look like an empty queue.
 */
public final class ThreadsReport {

    private static final String INDENT = "  ";

    private ThreadsReport() {
    }

    /**
     * Builds the summary shown by {@code /tuff threads}.
     *
     * @param status current bootstrap status
     * @return the report lines, or the reason there is nothing to report
     */
    public static List<String> overview(final ThreadsStatus status) {
        return switch (status.state()) {
            case NOT_INITIALISED -> List.of(
                "The Tuff thread layer has not been initialized yet."
            );
            case DISABLED -> disabled();
            case FAILED -> List.of(
                "The Tuff thread layer failed to start; the server is running without it.",
                INDENT + "The cause is in the server log, look for the \"Tuff/Threads\" logger."
            );
            case RUNNING -> running(status);
        };
    }

    private static List<String> disabled() {
        return List.of(
            "Tuff thread management is disabled in config/tuff.yml (thread-management.enabled: false).",
            INDENT + "Set it to true and restart the server to observe the server thread pools."
        );
    }

    private static List<String> running(final ThreadsStatus status) {
        final @Nullable ResourceSnapshot snapshot = status.snapshot();
        final @Nullable ThreadManagementSettings settings = status.settings();
        if (snapshot == null || settings == null) {
            return List.of(
                "Tuff thread layer: running, but no snapshot has been taken yet.",
                INDENT + "Try again in a second; the metrics sampler takes at most one pass per second."
            );
        }

        final List<String> lines = new ArrayList<>();
        lines.add("Tuff thread layer: " + (anyPoolManaged(snapshot) ? "running, some pools managed"
            : "running, all pools observed only"));
        lines.add(INDENT + "cores: " + snapshot.usableCores() + " usable after the reserve, "
            + snapshot.reservedCores() + " reserved; " + snapshot.totalThreads()
            + " registered thread(s) (peak " + snapshot.peakThreads() + ')');
        lines.add(INDENT + "pools: " + snapshot.pools().size() + " registered");

        final int width = idWidth(snapshot.pools());
        for (final PoolSnapshot pool : snapshot.pools()) {
            lines.add(describePool(pool, width));
            lines.addAll(describeRegions(pool));
        }
        if (snapshot.pools().isEmpty()) {
            lines.add(INDENT + INDENT + (status.metricsEnabled()
                ? "no pool is registered"
                : "the pool list is unavailable while metrics are disabled"));
        }

        if (!status.metricsEnabled()) {
            lines.add(INDENT + "metrics: disabled (thread-management.metrics.enabled: false); pool counters and "
                + "the thread list are not collected");
        }
        lines.add(INDENT + "degraded: " + (status.metricsEnabled()
            ? describeReasons(snapshot.degradedReasons())
            : "unknown (metrics are disabled)"));
        lines.add(INDENT + "unknown threads: " + describeUnregistered(status.unregisteredThreads()));
        lines.add(INDENT + "settings: reserve " + settings.resources().reserveCores() + " core(s), region scheduler "
            + formatScheduler(settings.regionExecution().scheduler())
            + ", region threads " + describeCount(settings.regionExecution().threads())
            + "; changes take effect after a restart");
        return List.copyOf(lines);
    }

    private static boolean anyPoolManaged(final ResourceSnapshot snapshot) {
        for (final PoolSnapshot pool : snapshot.pools()) {
            if (pool.coverage().managesThreads()) {
                return true;
            }
        }
        return false;
    }

    private static String describePool(final PoolSnapshot pool, final int width) {
        final PoolSpec spec = pool.spec();
        final StringBuilder out = new StringBuilder(INDENT).append(INDENT);
        out.append(pad(spec.id().value(), width)).append("  ").append(pool.coverage());
        out.append("  alive ").append(pool.aliveThreads());
        out.append("  active ").append(describeCount(pool.activeThreads()));
        out.append("  idle ").append(describeCount(pool.idleThreads()));
        if (pool.cpuLoad() >= 0.0d) {
            out.append("  cpu ").append(Math.round(pool.cpuLoad() * 100.0d)).append('%');
        }
        return out.toString();
    }

    /**
     * Describes the region tick health of a pool, when the probe reported any.
     *
     * <p>Two lines on purpose: the first is the one an operator scans for ("is a region late?"), the
     * second carries the detail that explains it. A pool that does not tick regions, or a pass that could
     * not read them, prints nothing at all - an empty "regions 0" line on the chunk pool would be noise.
     *
     * @param pool pool snapshot
     * @return the lines to add, empty when there is nothing to say
     */
    private static List<String> describeRegions(final PoolSnapshot pool) {
        final Optional<RegionPoolMetrics> metrics = pool.regionMetrics();
        if (metrics.isEmpty()) {
            return List.of();
        }
        final RegionPoolMetrics region = metrics.get();
        final String headline = INDENT + INDENT
            + "regions " + region.regions()
            + " / avg MSPT " + describeMspt(region.averageMspt5s())
            + " / min TPS " + describeTps(region.minimumTps())
            + " / lag " + Math.round(region.lagFraction() * 100.0d) + "% ("
            + region.laggingRegions() + " of " + region.regions() + ')';
        final String detail = INDENT + INDENT
            + "ticks: 5s max " + describeMspt(region.maximumMspt5s())
            + ", 15s avg " + describeMspt(region.averageMspt15s())
            + " max " + describeMspt(region.maximumMspt15s())
            + "; entities " + region.entities()
            + " / players " + region.players()
            + " / chunks " + region.chunks();
        return List.of(headline, detail);
    }

    private static String describeMspt(final double millis) {
        return millis < 0.0d ? "unknown" : String.format(Locale.ROOT, "%.1f ms", millis);
    }

    private static String describeTps(final double tps) {
        return tps < 0.0d ? "unknown" : String.format(Locale.ROOT, "%.1f", tps);
    }

    private static int idWidth(final List<PoolSnapshot> pools) {
        int width = 1;
        for (final PoolSnapshot pool : pools) {
            width = Math.max(width, pool.id().value().length());
        }
        return width;
    }

    private static String pad(final String value, final int width) {
        return value.length() >= width ? value : value + " ".repeat(width - value.length());
    }

    private static String describeCount(final int count) {
        return count < 0 ? "unknown" : Integer.toString(count);
    }

    private static String describeUnregistered(final int count) {
        if (count < 0) {
            return "n/a (not measured; it needs a JVM wide stack dump, so run it from the console)";
        }
        return Integer.toString(count);
    }

    private static String describeReasons(final Collection<DegradedReason> reasons) {
        if (reasons.isEmpty()) {
            return "none";
        }
        final List<String> names = new ArrayList<>(reasons.size());
        for (final DegradedReason reason : reasons) {
            names.add(reason.name());
        }
        names.sort(null);
        return String.join(", ", names);
    }

    private static String formatScheduler(final ThreadManagementSettings.RegionScheduler scheduler) {
        return scheduler.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
