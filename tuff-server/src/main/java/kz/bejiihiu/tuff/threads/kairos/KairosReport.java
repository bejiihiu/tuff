package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import org.jspecify.annotations.Nullable;

/**
 * Turns a {@link KairosSnapshot} into the lines {@code /tuff scheduler} prints.
 *
 * <p>A pure function of its arguments, like every other command report in this codebase: all branches -
 * EDF, work stealing, Kairos with and without a snapshot, no thread layer at all - are testable without
 * a server. The command adds colour; this class owns the wording.
 *
 * <p>EDF and work stealing deliberately print that they have no fund telemetry. They do not have a
 * fund, a supervisor or a wait queue this project can read, so inventing a number for them would be
 * worse than the honest sentence. What they do print is the configured thread count, which is the one
 * setting an operator can act on without Kairos.
 */
public final class KairosReport {

    private static final String INDENT = "  ";

    private KairosReport() {
    }

    /**
     * Builds the report of one scheduler.
     *
     * @param scheduler         active scheduler name ({@code EDF}, {@code WORK_STEALING}, {@code KAIROS}),
     *                          or {@code null} when the thread layer cannot name one
     * @param configuredThreads configured region thread count, or {@code -1} for automatic
     * @param snapshot          current Kairos snapshot, or {@code null} when Kairos is not running
     * @return the report lines, never empty
     */
    public static List<String> overview(
        final @Nullable String scheduler,
        final int configuredThreads,
        final @Nullable KairosSnapshot snapshot
    ) {
        if (snapshot != null) {
            return kairos(snapshot);
        }
        if (scheduler != null && "KAIROS".equalsIgnoreCase(scheduler)) {
            return List.of(
                "Tuff scheduler: Kairos (experimental) - not started",
                INDENT + "the scheduler is selected in the configuration, but no Kairos scheduler exists in "
                    + "this process yet; the startup log says whether it refused or has not started"
            );
        }
        if (scheduler == null) {
            return List.of(
                "Tuff scheduler: unknown",
                INDENT + "the thread layer is disabled or failed, so it cannot name the active scheduler; "
                    + "/tuff threads says which of the two it is"
            );
        }
        return List.of(
            "Tuff scheduler: " + scheduler,
            INDENT + "no fund telemetry: only Kairos keeps a tick fund, tick wait ages and a supervisor thread",
            INDENT + "configured region threads: " + describeThreads(configuredThreads)
                + " (threaded-regions.threads); a change needs a restart"
        );
    }

    private static List<String> kairos(final KairosSnapshot snapshot) {
        final List<String> lines = new ArrayList<>(6);
        lines.add("Tuff scheduler: Kairos (experimental) - status: "
            + snapshot.status().name().toLowerCase(Locale.ROOT));
        lines.add(INDENT + "fund: min " + snapshot.minThreads()
            + ", live " + snapshot.liveThreads()
            + ", target " + snapshot.targetThreads()
            + ", max " + snapshot.maxThreads()
            + "; elasticity " + (snapshot.elastic() ? "on" : "off")
            + ", ceiling source: " + snapshot.budgetSource());
        lines.add(INDENT + "queue: " + snapshot.queuedTicks() + " queued, "
            + snapshot.awaitingTicks() + " awaiting, "
            + snapshot.executingTicks() + " executing; tick wait avg "
            + describeMs(snapshot.averageWaitMillis()) + ", max "
            + describeMs(snapshot.longestWaitMillis()) + "; overdue "
            + snapshot.overdueTicks() + " (threshold " + snapshot.overdueThresholdMillis() + " ms"
            + (snapshot.overdueTicks() > 0
                ? ", oldest " + snapshot.oldestOverdueMillis() + " ms" : "")
            + ')');
        lines.add(INDENT + "supervisor: " + describeSupervisor(snapshot.supervisor(), snapshot.elastic())
            + "; last decision " + describeDecision(snapshot));
        lines.add(INDENT + "fund events: spawned " + snapshot.spawnCount()
            + ", retired " + snapshot.retireCount());
        lines.add(INDENT + "degraded: " + describeReasons(snapshot.degradedReasons()));
        return List.copyOf(lines);
    }

    private static String describeSupervisor(
        final KairosSnapshot.SupervisorState state,
        final boolean elastic
    ) {
        return switch (state) {
            case OFF -> elastic ? "off (no headroom above the floor)" : "off (fixed fund, no control thread)";
            case NOT_STARTED -> "not started";
            case RUNNING -> "running";
            case STOPPED -> "stopped";
            case DEAD -> "dead (the fund keeps its last size)";
        };
    }

    private static String describeDecision(final KairosSnapshot snapshot) {
        final String kind = snapshot.lastDecision().name().toLowerCase(Locale.ROOT);
        return kind + " (" + snapshot.lastDecisionDetail() + ')';
    }

    private static String describeReasons(final List<KairosSnapshot.Degraded> reasons) {
        if (reasons.isEmpty()) {
            return "none";
        }
        final StringJoiner names = new StringJoiner(", ");
        for (final KairosSnapshot.Degraded reason : reasons) {
            names.add(reason.name());
        }
        return names.toString();
    }

    private static String describeThreads(final int threads) {
        return threads > 0 ? Integer.toString(threads) : "unknown (automatic)";
    }

    private static String describeMs(final double millis) {
        return String.format(Locale.ROOT, "%.1f ms", millis);
    }
}
