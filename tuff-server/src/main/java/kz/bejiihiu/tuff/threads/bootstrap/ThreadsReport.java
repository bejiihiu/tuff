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
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.pressure.TickPressureController;
import kz.bejiihiu.tuff.threads.pressure.TickPressureSnapshot;
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
 *
 * <p>The report is a pure function of a status the refresher already measured, so it is safe to build on
 * the global region tick thread Folia runs console commands on. When there is no sample yet it says so
 * instead of printing an empty table.
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
                "Tuff thread layer: warming up (no sample yet).",
                INDENT + "The refresher takes one pass per second; run the command again in a moment."
            );
        }

        final List<String> lines = new ArrayList<>();
        // the header is the configuration's statement, not the measured coverage: the refresher's
        // service pool is L2 because the manager creates that thread, and calling that "managed" would
        // tell an operator that Folia's region ticks are managed when they are not
        lines.add("Tuff thread layer: " + (settings.anyManagementEnabled() ? "running, some pools managed"
            : "running, all pools observed only"));
        lines.add(INDENT + "cores: " + snapshot.usableCores() + " usable after the reserve, "
            + snapshot.reservedCores() + " reserved; " + snapshot.totalThreads()
            + " registered thread(s) (peak " + snapshot.peakThreads() + ')');
        lines.add(INDENT + "reserve: " + describeShedding(snapshot.shedding(), settings));
        lines.add(INDENT + "sample: taken " + describeAge(status.sampleAgeMillis()));
        lines.add(INDENT + "pools: " + snapshot.pools().size() + " registered");

        final int width = idWidth(snapshot.pools());
        for (final PoolSnapshot pool : snapshot.pools()) {
            lines.add(describePool(pool, width));
            lines.addAll(describeRegions(pool));
        }
        if (snapshot.pools().isEmpty()) {
            if (!status.metricsEnabled()) {
                lines.add(INDENT + INDENT + "the pool list is unavailable while metrics are disabled");
            } else if (!settings.metrics().perPool()) {
                lines.add(INDENT + INDENT + "the pool list is off (thread-management.metrics.per-pool: false); "
                    + "the totals and the thread list are still published");
            } else {
                lines.add(INDENT + INDENT + "no pool is registered");
            }
        }

        if (!status.metricsEnabled()) {
            lines.add(INDENT + "metrics: disabled (thread-management.metrics.enabled: false); pool counters and "
                + "the thread list are not collected");
        }
        lines.add(INDENT + describePressure(status, settings));
        lines.add(INDENT + "degraded: " + (status.metricsEnabled()
            ? describeReasons(snapshot.degradedReasons())
            : "unknown (metrics are disabled)"));
        lines.add(INDENT + "unknown threads: "
            + describeUnregistered(status.unregisteredThreads(), status.metricsEnabled()));
        lines.add(INDENT + "settings: reserve " + settings.resources().reserveCores() + " core(s), region scheduler "
            + formatScheduler(settings.regionExecution().scheduler())
            + ", region threads " + describeCount(settings.regionExecution().threads())
            + "; changes take effect after a restart");
        return List.copyOf(lines);
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

    /**
     * Describes the compute pool's tick pressure decision.
     *
     * <p>One line, always printed while the layer runs: "disabled" is information too, and an operator
     * who wonders why the compute pool never yields needs to see that the switch is off without
     * reading the file. The line carries the decision's reason so a reserve that sits at its ceiling
     * does not look like a stuck controller.
     *
     * @param status   current status
     * @param settings effective settings
     * @return the line, without the leading indent
     */
    private static String describePressure(final ThreadsStatus status, final ThreadManagementSettings settings) {
        if (!settings.tickPressure().enabled()) {
            return "pressure: disabled (thread-management.async-compute.tick-pressure.enabled: false)";
        }
        final @Nullable TickPressureSnapshot pressure = status.pressure();
        if (pressure == null || !pressure.signal().measured()) {
            return "pressure: enabled, waiting for a region reading";
        }
        return "pressure: " + (pressure.state() == TickPressureController.State.PRESSURE ? "yielding" : "normal")
            + ", reserve " + pressure.reserve() + " of " + pressure.maxReserve() + " core(s)"
            + ", budget " + pressure.appliedBudget() + " applied of " + pressure.poolBudget() + " pool core(s)"
            + ", " + pressure.usableCores() + " usable"
            + "; lag " + Math.round(pressure.signal().lagFraction() * 100.0d) + '%'
            + ", min TPS " + describeTps(pressure.signal().minimumTps())
            + ", max MSPT " + describeMspt(pressure.signal().maximumMspt())
            + " (" + pressure.reason().name() + ')';
    }

    /**
     * Describes the critical reserve and the load shedding state in one line.
     *
     * <p>The controller's report already spells out the numbers and the pools, so it is printed as is
     * while the policy is on. With the policy off the line still explains the reserve, because a pool
     * smaller than its configuration is the question an operator asks first, and "off" is part of the
     * answer.
     *
     * @param shedding report carried by the snapshot
     * @param settings effective settings, the source of the switch
     * @return the line body
     */
    private static String describeShedding(
        final SheddingReport shedding,
        final ThreadManagementSettings settings
    ) {
        final String held = shedding.criticalReserveCores() + " core(s) held for the critical lanes";
        if (!settings.resources().loadShedding()) {
            return held + "; load shedding off (thread-management.resources.load-shedding: false)";
        }
        return shedding.summary();
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

    private static String describeUnregistered(final int count, final boolean metricsEnabled) {
        if (count >= 0) {
            return Integer.toString(count);
        }
        return metricsEnabled
            ? "n/a (no sample yet)"
            : "n/a (metrics collection is disabled)";
    }

    private static String describeAge(final long millis) {
        if (millis < 0L) {
            return "unknown";
        }
        if (millis < 1_000L) {
            return millis + " ms ago";
        }
        return String.format(Locale.ROOT, "%.1f s ago", millis / 1_000.0d);
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
