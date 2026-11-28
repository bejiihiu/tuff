package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import org.jspecify.annotations.Nullable;

/**
 * Turns a {@link KairosSnapshot} into the lines {@code /tuff scheduler} prints.
 *
 * <p>A pure function of its arguments, like every other command report in this codebase: all branches -
 * EDF, work stealing, Kairos with and without a snapshot, no thread layer at all - are testable without
 * a server. The command adds colour; this class owns the wording.
 *
 * <p>The command has one overview and four details. {@code leases} ranks the recent permit waits,
 * {@code budget} explains where the CPU went and why a state is degraded, {@code reset} starts a new
 * measurement window, and {@code help} lists the grammar. Everything the details print comes from a
 * snapshot the command already had to take; none of them measures anything itself.
 *
 * <p>EDF and work stealing deliberately print that they have no fund telemetry. They do not have a
 * fund, a supervisor or a wait queue this project can read, so inventing a number for them would be
 * worse than the honest sentence. What they do print is the configured thread count, which is the one
 * setting an operator can act on without Kairos.
 */
public final class KairosReport {

    private static final String INDENT = "  ";

    /**
     * The deduction terms of the thread layer's budget ledger line.
     *
     * <p>The thread layer publishes the individual deductions as one sentence rather than as separate
     * accessors, so the budget view reads them back from the same authoritative line it prints. A line
     * this pattern does not match is reported as {@code n/a} instead of being guessed at.
     */
    private static final Pattern LEDGER_TERMS = Pattern.compile(
        "region (\\d+) \\+ chunk (\\d+) \\+ io (\\d+) \\+ reserve (\\d+)(?: \\+ critical (\\d+))?"
    );

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

    /**
     * Builds the ranking of recent permit waits.
     *
     * <p>Samples are ranked by the worse of their two waits, because an operator asks "which region
     * suffered most", not "which queue was longer". Ties fall back to the tick id so the order is stable
     * for a given snapshot.
     *
     * @param scheduler active scheduler name, or {@code null} when it is unknown
     * @param snapshot  current Kairos snapshot, or {@code null} when Kairos is not running
     * @param limit     how many samples to print, clamped to at least one
     * @return the report lines, never empty
     */
    public static List<String> leases(
        final @Nullable String scheduler,
        final @Nullable KairosSnapshot snapshot,
        final int limit
    ) {
        if (snapshot == null) {
            return List.of(
                "Tuff scheduler: leases",
                INDENT + "no permit waits: only Kairos keeps a tick fund" + describeActive(scheduler)
            );
        }
        final KairosSnapshot.LeaseWait leaseWait = snapshot.leaseWait();
        final List<String> lines = new ArrayList<>(4);
        lines.add("Tuff scheduler: leases (recent permit waits)");
        lines.add(INDENT + "grants: " + describeGrants(leaseWait));
        final List<KairosSnapshot.LeaseWait.Sample> samples = new ArrayList<>(leaseWait.recent());
        if (samples.isEmpty()) {
            lines.add(INDENT + "recent waits: none yet; the ring fills on the first grant");
            return List.copyOf(lines);
        }
        samples.sort(Comparator.comparingLong(KairosSnapshot.LeaseWait.Sample::worstWaitMillis)
            .reversed()
            .thenComparing(Comparator.comparingLong(KairosSnapshot.LeaseWait.Sample::tickId).reversed()));
        final int shown = Math.min(Math.max(1, limit), samples.size());
        lines.add(INDENT + "worst waits: top " + shown + " of " + samples.size() + " recent sample(s)");
        for (int index = 0; index < shown; index++) {
            lines.add(INDENT + INDENT + describeSample(index + 1, samples.get(index)));
        }
        return List.copyOf(lines);
    }

    /**
     * Builds the budget view: the thread layer ledger, its deductions, the compute floor and the Kairos
     * ceiling, plus the reason for any degraded state.
     *
     * <p>Every number is copied from a report the caller already holds. A value that the thread layer
     * does not publish - because it is disabled, failed or has not sampled yet - is printed as
     * {@code n/a} with the reason, never as a zero that would look measured.
     *
     * @param input the cached reports and configured limits to print
     * @return the report lines, never empty
     */
    public static List<String> budget(final BudgetInput input) {
        final List<String> lines = new ArrayList<>(8);
        lines.add("Tuff scheduler: budget");
        final @Nullable ResourceSnapshot threads = input.threads();
        if (threads == null) {
            lines.add(INDENT + "thread layer: " + (input.threadsNote() == null
                ? "not available, so the ledger, the deductions and usable cores are n/a"
                : input.threadsNote()));
        } else {
            lines.add(INDENT + "thread layer: running, sample taken " + describeMillis(input.sampleAgeMillis()));
            lines.add(INDENT + "ledger: " + threads.budgetLedger().orElse(
                "n/a (the thread layer has not published a ledger yet)"));
            final @Nullable String deductions = threads.budgetLedger()
                .map(KairosReport::describeDeductions)
                .orElse(null);
            lines.add(INDENT + "deductions: " + (deductions == null
                ? "n/a (the ledger line above does not expose its terms in a form this command can read)"
                : deductions));
            lines.add(INDENT + "usable: " + threads.usableCores() + " core(s) after every deduction; "
                + threads.reservedCores() + " core(s) reserved by configuration, "
                + threads.criticalReserveCores() + " held for the critical lanes");
        }
        lines.add(INDENT + describeCompute(input, threads));
        lines.add(INDENT + describeKairosBudget(input.kairos(), input.scheduler()));
        lines.add(INDENT + "degraded: " + describeBudgetReasons(threads, input.kairos()));
        return List.copyOf(lines);
    }

    /**
     * Builds the confirmation of a counter reset.
     *
     * <p>One line on purpose: the command is a measurement reset, not a report, and an operator who
     * runs it wants an acknowledgement rather than a table. A refused reset says why, because "not
     * reset" without a reason is the kind of answer that sends somebody looking in the log.
     *
     * @param applied   whether a running fund zeroed its counters
     * @param scheduler active scheduler name, or {@code null} when it is unknown
     * @return the confirmation, never empty
     */
    public static List<String> reset(final boolean applied, final @Nullable String scheduler) {
        if (!applied) {
            return List.of(
                "Tuff scheduler: counters not reset: Kairos is not running" + describeActive(scheduler)
                    + ", so there are no counters to zero"
            );
        }
        return List.of(
            "Tuff scheduler: counters reset (spawns, retires, executed ticks, affinity hits and misses, steals, "
                + "catch-up bursts and ticks, permit waits and their worst values, overdue warnings); "
                + "the fund, the regions and the permits are untouched."
        );
    }

    /**
     * Builds the scheduler command usage.
     *
     * @return the help lines, never empty
     */
    public static List<String> help() {
        return List.of(
            "Tuff scheduler commands:",
            INDENT + "/tuff scheduler                fund, queue, supervisor, idle work, leases and degraded state",
            INDENT + "/tuff scheduler leases [n]     worst recent permit waits (n 1..20, default 5)",
            INDENT + "/tuff scheduler budget         budget ledger, usable cores and the reason for any degraded state",
            INDENT + "/tuff scheduler reset          zero the scheduler's A/B counters; the fund and the permits stay",
            INDENT + "/tuff scheduler help           this list"
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
            + snapshot.executingTicks() + " executing ("
            + snapshot.scheduledTicks() + " scheduled); tick wait avg "
            + describeMs(snapshot.averageWaitMillis()) + ", max "
            + describeMs(snapshot.longestWaitMillis()) + "; overdue "
            + snapshot.overdueTicks() + " (threshold " + snapshot.overdueThresholdMillis() + " ms"
            + (snapshot.overdueTicks() > 0
                ? ", oldest " + snapshot.oldestOverdueMillis() + " ms" : "")
            + "); warnings " + snapshot.overdueWarns() + " ("
            + snapshot.suppressedOverdueWarns() + " suppressed)");
        lines.add(INDENT + "supervisor: " + describeSupervisor(snapshot.supervisor(), snapshot.elastic())
            + "; last decision " + describeDecision(snapshot));
        lines.add(INDENT + "fund events: spawned " + snapshot.spawnCount()
            + ", retired " + snapshot.retireCount());
        lines.add(INDENT + "ticks: executed " + snapshot.executedTicks()
            + ", affinity hits " + snapshot.affinityHits()
            + ", misses " + snapshot.affinityMisses()
            + ", steals " + snapshot.stolenTicks()
            + ", catch-up " + snapshot.catchUpBursts() + " burst(s) / "
            + snapshot.catchUpTicks() + " tick(s)");
        lines.add(INDENT + "idle work: " + describeIdleWork(snapshot.idleWork()));
        lines.add(INDENT + "leases: " + describeLeaseWait(snapshot.leaseWait()));
        lines.add(INDENT + "degraded: " + describeReasons(snapshot.degradedReasons()));
        return List.copyOf(lines);
    }

    private static String describeGrants(final KairosSnapshot.LeaseWait leaseWait) {
        if (leaseWait.grants() == 0L) {
            return "none measured yet; the ring fills on the first permit grant";
        }
        final StringBuilder text = new StringBuilder(96);
        text.append(leaseWait.grants()).append(" measured, ");
        if (leaseWait.armed()) {
            text.append(leaseWait.timeouts()).append(" over the ")
                .append(leaseWait.timeoutMillis()).append(" ms timeout");
        } else {
            text.append("timeout off (lease.timeout-seconds: 0)");
        }
        text.append("; worst queued ").append(leaseWait.queuedMaxMillis())
            .append(" ms, worst parked ").append(leaseWait.parkedMaxMillis()).append(" ms");
        return text.toString();
    }

    private static String describeSample(final int rank, final KairosSnapshot.LeaseWait.Sample sample) {
        final String region = sample.regionId() >= 0L
            ? "region " + sample.regionId()
            : "region unknown";
        return "#" + rank + ' ' + region + ", tick " + sample.tickId()
            + ": queued " + sample.queuedMillis() + " ms"
            + ", parked " + sample.parkedMillis() + " ms"
            + ", age " + describeAge(sample.ageMillis());
    }

    private static String describeCompute(final BudgetInput input, final @Nullable ResourceSnapshot threads) {
        if (input.computeFloor() <= 0) {
            return "compute: not running (async-compute.enabled is false or the engine failed), so the floor "
                + "and the effective budget are n/a";
        }
        final String maximum = input.computeMax() == -1 ? "auto" : Integer.toString(input.computeMax());
        final StringBuilder text = new StringBuilder(128);
        text.append("compute: floor ").append(input.computeFloor()).append(" worker(s), max ")
            .append(maximum).append("; ");
        if (threads == null) {
            text.append("effective budget n/a (no thread layer ledger to resolve it against)");
            return text.toString();
        }
        final @Nullable PoolSnapshot pool = threads.pool(PoolId.COMPUTE).orElse(null);
        if (pool == null) {
            text.append("effective budget n/a (the compute pool is not registered with the thread layer; "
                + "execution-pools.compute.managed is off)");
            return text.toString();
        }
        text.append("effective budget ").append(pool.spec().budget().resolveCores(threads.usableCores()))
            .append(" core(s) (the pool's declared budget resolved against the ledger's usable cores)");
        return text.toString();
    }

    private static String describeKairosBudget(
        final @Nullable KairosSnapshot kairos,
        final @Nullable String scheduler
    ) {
        if (kairos == null) {
            return "kairos: not running" + describeActive(scheduler)
                + ", so min, max, the ceiling and the budget source are n/a";
        }
        return "kairos: min " + kairos.minThreads() + ", max " + kairos.maxThreads()
            + " tick thread(s) (the resolved ceiling); budget source: " + kairos.budgetSource();
    }

    private static String describeBudgetReasons(
        final @Nullable ResourceSnapshot threads,
        final @Nullable KairosSnapshot kairos
    ) {
        final List<String> reasons = new ArrayList<>(4);
        if (threads != null) {
            for (final DegradedReason reason : threads.degradedReasons()) {
                reasons.add(reason.name() + " (" + describeReason(reason) + ')');
            }
        }
        if (kairos != null) {
            for (final KairosSnapshot.Degraded reason : kairos.degradedReasons()) {
                reasons.add("kairos " + reason.name() + " (" + describeReason(reason) + ')');
            }
        }
        if (!reasons.isEmpty()) {
            reasons.sort(null);
            return String.join("; ", reasons);
        }
        if (threads == null && kairos == null) {
            return "unknown (no thread layer snapshot and no Kairos fund to read)";
        }
        return "none";
    }

    private static @Nullable String describeDeductions(final String ledger) {
        final Matcher matcher = LEDGER_TERMS.matcher(ledger);
        if (!matcher.find()) {
            return null;
        }
        final String critical = matcher.group(5);
        return "region " + matcher.group(1)
            + ", chunk " + matcher.group(2)
            + ", io " + matcher.group(3)
            + ", reserve " + matcher.group(4) + " core(s)"
            + (critical == null ? "" : ", critical " + critical);
    }

    private static String describeReason(final DegradedReason reason) {
        return switch (reason) {
            case OBSERVE_ONLY -> "the pool is observed only";
            case FEATURE_DISABLED -> "the feature that would manage this pool is off";
            case BUDGET_EXHAUSTED -> "the reserves and minimums do not fit the usable cores";
            case TOPOLOGY_UNKNOWN -> "the processor topology could not be measured";
            case SCHEDULER_LIMITATION -> "the pool owner cannot expose the control this layer needs";
            case THREAD_CREATION_FAILED -> "a thread could not be created";
            case POOL_UNRESPONSIVE -> "a pool stopped answering probes";
            case NUMA_UNAVAILABLE -> "NUMA or affinity information is unavailable";
            case CONFIG_CONFLICT -> "a legacy key disagrees with the unified one";
            case METRICS_UNAVAILABLE -> "metrics collection is disabled";
            case CRITICAL_POOL_PRESSURE -> "a critical pool is short of its guaranteed share";
            case CRITICAL_OVERCOMMIT -> "the critical pools are promised more than the ledger has";
            case LOAD_SHEDDING -> "capacity is temporarily withheld to keep a critical lane alive";
            case NOT_RUNNING -> "the manager is not running";
        };
    }

    private static String describeReason(final KairosSnapshot.Degraded reason) {
        return switch (reason) {
            case NO_HEADROOM -> "the resolved ceiling is not above the floor";
            case SUPERVISOR_DEAD -> "the control thread is gone, so the fund keeps its size";
            case QUEUED_OVERDUE -> "at least one tick is past the wait threshold right now";
        };
    }

    /**
     * Describes the idle path: what it is allowed to do, what it did and whether the guard turned it
     * off. "off" tools are named explicitly so an operator can tell a disabled feature from a broken
     * one.
     *
     * @param idle idle report of the snapshot
     * @return one line without the leading indent
     */
    private static String describeIdleWork(final KairosSnapshot.IdleWork idle) {
        if (!idle.enabled()) {
            return "off (idle-work.enabled is false)";
        }
        if (!idle.midTick() && !idle.utmiJobs()) {
            return "off (mid-tick and utmi-jobs are both false)";
        }
        final StringBuilder text = new StringBuilder(128);
        text.append("on (mid-tick ").append(idle.midTick() ? "on" : "off")
            .append(", utmi jobs ").append(idle.utmiJobs() ? "on" : "off");
        if (idle.utmiJobs() && !idle.sourcePresent()) {
            text.append(", no source");
        }
        text.append("); ").append(idle.jobsTaken()).append(" job(s), ")
            .append(idle.midTickPasses()).append(" mid-tick pass(es), ")
            .append(idle.preemptions()).append(" preemption(s), ")
            .append(TimeUnit.NANOSECONDS.toMillis(idle.idleWorkNanos())).append(" ms spent, ")
            .append(String.format(Locale.ROOT, "%.1f", idle.shareOfIdlePercent()))
            .append("% of idle, added lag max ").append(idle.addedTickLagMaxMillis()).append(" ms; ");
        if (idle.disabled() == KairosSnapshot.IdleWork.Disabled.NONE) {
            text.append("self-protection: none");
        } else {
            text.append("self-protection: disabled (idle work added more lag than its budget)");
        }
        return text.toString();
    }

    private static String describeLeaseWait(final KairosSnapshot.LeaseWait leaseWait) {
        final StringBuilder text = new StringBuilder(64);
        text.append(leaseWait.grants()).append(" measured, wait max queued ")
            .append(leaseWait.queuedMaxMillis()).append(" ms / parked ")
            .append(leaseWait.parkedMaxMillis()).append(" ms");
        if (leaseWait.armed()) {
            text.append(", ").append(leaseWait.timeouts())
                .append(" over the ").append(leaseWait.timeoutMillis()).append(" ms timeout");
        } else {
            text.append(", timeout off");
        }
        return text.toString();
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

    /**
     * Returns the parenthetical that names the active scheduler, or nothing when it is unknown.
     *
     * @param scheduler active scheduler name, or {@code null}
     * @return {@code " (the active scheduler is EDF)"} or an empty string
     */
    private static String describeActive(final @Nullable String scheduler) {
        return scheduler == null ? "" : " (the active scheduler is " + scheduler + ')';
    }

    private static String describeThreads(final int threads) {
        return threads > 0 ? Integer.toString(threads) : "unknown (automatic)";
    }

    private static String describeMs(final double millis) {
        return String.format(Locale.ROOT, "%.1f ms", millis);
    }

    private static String describeMillis(final long millis) {
        return millis < 0L ? "unknown" : millis + " ms ago";
    }

    private static String describeAge(final long millis) {
        if (millis < 1_000L) {
            return millis + " ms ago";
        }
        return String.format(Locale.ROOT, "%.1f s ago", millis / 1_000.0d);
    }

    /**
     * The cached reports one budget view needs.
     *
     * @param scheduler       active scheduler name, or {@code null} when the thread layer cannot name one
     * @param threads         latest thread layer snapshot, or {@code null} when there is none
     * @param sampleAgeMillis age of that snapshot, or {@code -1} when there is none
     * @param threadsNote     why there is no snapshot, {@code null} when there is one
     * @param kairos          current Kairos snapshot, or {@code null} when Kairos is not running
     * @param computeFloor    configured minimum workers of the compute engine, or {@code 0} when it is off
     * @param computeMax      configured maximum workers, {@code -1} for automatic
     */
    public record BudgetInput(
        @Nullable String scheduler,
        @Nullable ResourceSnapshot threads,
        long sampleAgeMillis,
        @Nullable String threadsNote,
        @Nullable KairosSnapshot kairos,
        int computeFloor,
        int computeMax
    ) {
    }
}
