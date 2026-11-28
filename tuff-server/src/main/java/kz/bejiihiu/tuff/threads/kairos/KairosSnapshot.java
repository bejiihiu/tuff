package kz.bejiihiu.tuff.threads.kairos;

import java.util.List;
import java.util.Objects;

/**
 * One consistent moment of the Kairos fund, taken for {@code /tuff scheduler} and {@code /tuff threads}.
 *
 * <p>The snapshot is built from the scheduler's own counters and a short pass over the two tick
 * queues under the schedule lock. It never walks regions, never touches a tick body and never blocks on
 * a runner, which is what makes it safe to build on the global region tick thread the console command
 * runs on.
 *
 * <p>Every age in the snapshot is the age of a tick's {@code scheduledStart}: a positive number means
 * the deadline has already passed and the tick is still waiting. A tick whose deadline is still in the
 * future contributes zero, so an empty queue and a queue of early ticks both read as zero wait.
 *
 * <p>{@code budgetSource} is the text the scheduler resolved at start. It is {@code null} before the
 * scheduler starts; printing the operator's ceiling there instead of a resolved one would hide a
 * degraded budget behind a number that was never applied.
 *
 * @param elastic                 whether the fund may grow and shrink at all
 * @param minThreads              floor of the fund
 * @param maxThreads              ceiling the scheduler resolved at start
 * @param liveThreads             started and not yet exited runners
 * @param targetThreads           fund size the last supervisor pass asked for
 * @param queuedTicks             ticks waiting for a permit
 * @param awaitingTicks           runners holding a permit and waiting for the deadline
 * @param executingTicks          runners inside {@code runTick}
 * @param scheduledTicks          ticks the scheduler holds in total, including executing ones
 * @param overdueTicks            queued ticks whose {@code scheduledStart} is older than the threshold
 * @param oldestOverdueMillis     age of the oldest overdue tick, in milliseconds, or zero when none
 * @param averageWaitMillis       mean positive wait across queued and awaiting ticks, in milliseconds
 * @param longestWaitMillis       longest positive wait across queued and awaiting ticks, in milliseconds
 * @param overdueThresholdMillis  wait above which a tick counts as overdue, in milliseconds
 * @param supervisor              state of the control thread
 * @param lastDecision            what the last supervisor pass decided
 * @param lastDecisionDetail      the numbers behind that decision
 * @param spawnCount              runners started since the process began
 * @param retireCount             runners retired since the process began
 * @param budgetSource            where the ceiling came from, never {@code null}
 * @param degradedReasons         reasons the fund is not at its best, possibly empty
 * @param leaseWait               permit waits and the timeout they are compared against
 * @param affinityHits            ticks handed back to the runner that ran them last
 * @param stolenTicks             ticks another runner took out of a backlog
 * @param catchUpBursts           catch-up bursts the fund started
 * @param catchUpTicks            tick bodies run to recover a backlog
 * @param idleWork                the idle path: what it is allowed to do, what it did and why it stopped
 */
public record KairosSnapshot(
    boolean elastic,
    int minThreads,
    int maxThreads,
    int liveThreads,
    int targetThreads,
    int queuedTicks,
    int awaitingTicks,
    int executingTicks,
    int scheduledTicks,
    int overdueTicks,
    long oldestOverdueMillis,
    double averageWaitMillis,
    double longestWaitMillis,
    long overdueThresholdMillis,
    SupervisorState supervisor,
    Decision lastDecision,
    String lastDecisionDetail,
    long spawnCount,
    long retireCount,
    String budgetSource,
    List<Degraded> degradedReasons,
    LeaseWait leaseWait,
    long affinityHits,
    long stolenTicks,
    long catchUpBursts,
    long catchUpTicks,
    IdleWork idleWork
) {

    /**
     * Validates the snapshot.
     *
     * @throws NullPointerException if a reference or a reason is {@code null}
     */
    public KairosSnapshot {
        Objects.requireNonNull(supervisor, "supervisor");
        Objects.requireNonNull(lastDecision, "lastDecision");
        Objects.requireNonNull(lastDecisionDetail, "lastDecisionDetail");
        Objects.requireNonNull(budgetSource, "budgetSource");
        Objects.requireNonNull(leaseWait, "leaseWait");
        Objects.requireNonNull(idleWork, "idleWork");
        degradedReasons = List.copyOf(degradedReasons);
    }

    /**
     * Returns the health of the fund.
     *
     * @return {@link Status#DEGRADED} when at least one reason is present
     */
    public Status status() {
        return this.degradedReasons.isEmpty() ? Status.OK : Status.DEGRADED;
    }

    /**
     * Health of the fund as one word.
     */
    public enum Status {

        /**
         * Every reason list is empty.
         */
        OK,

        /**
         * At least one {@link Degraded} reason is present.
         */
        DEGRADED
    }

    /**
     * State of the control thread.
     */
    public enum SupervisorState {

        /**
         * The fund is fixed, so there is no supervisor by design.
         */
        OFF,

        /**
         * Elastic, but the scheduler has not started.
         */
        NOT_STARTED,

        /**
         * The thread is alive and making passes.
         */
        RUNNING,

        /**
         * The scheduler was halted.
         */
        STOPPED,

        /**
         * The scheduler started but the thread is gone.
         */
        DEAD
    }

    /**
     * What the last supervisor pass did.
     */
    public enum Decision {

        /**
         * A runner was added.
         */
        GROW,

        /**
         * A parked runner was retired.
         */
        SHRINK,

        /**
         * The fund kept its size.
         */
        IDLE
    }

    /**
     * Why the fund is not at its best.
     */
    public enum Degraded {

        /**
         * The resolved ceiling is not above the floor, so elasticity cannot do anything.
         */
        NO_HEADROOM,

        /**
         * The fund is elastic, started, and its supervisor thread is not alive.
         */
        SUPERVISOR_DEAD,

        /**
         * At least one queued tick is past the wait threshold right now.
         */
        QUEUED_OVERDUE
    }

    /**
     * Permit waits of the fund, and the timeout the operator asked to be warned about.
     *
     * <p>The maxima are the worst values since the scheduler started. They are best effort: two
     * runners racing for the same maximum may overwrite each other, and a metric that is occasionally
     * one sample short is worth more than an atomic on the tick path.
     *
     * @param grants             permits handed out and measured
     * @param queuedMaxMillis    longest wait between a deadline and its permit grant
     * @param parkedMaxMillis    longest wait between the later of grant and deadline and the body
     * @param timeouts           grants whose wait exceeded the timeout
     * @param timeoutMillis      configured timeout, or zero when only accounting is on
     */
    public record LeaseWait(
        long grants,
        long queuedMaxMillis,
        long parkedMaxMillis,
        long timeouts,
        long timeoutMillis
    ) {

        /**
         * Returns whether the operator asked to be warned about slow permits.
         *
         * @return {@code true} when a timeout is configured
         */
        public boolean armed() {
            return this.timeoutMillis > 0L;
        }
    }

    /**
     * State and cost of the idle path: the work a tick thread may do while it has nothing to tick.
     *
     * <p>Off is the default and reads as zero everywhere: no source is polled, no intermediate task is
     * drained and the counters stay at zero. When the path is on, the counters are what a measurement
     * needs to judge it: how much work was taken, how often a due tick interrupted it, and how much
     * lateness it caused. {@code addedTickLagMaxMillis} is measured only across a body that actually
     * followed idle work, so an unrelated slow tick is not blamed on it.
     *
     * <p>{@code disabled} is the self-protection, not the configuration: {@link Disabled#LAG_BUDGET}
     * means the path ran and added more lateness than {@code max-added-lag-millis}, so it turned itself
     * off until the region was stable again. A path that is off by configuration reports
     * {@link Disabled#NONE} and {@code enabled == false}.
     *
     * @param enabled               master switch from the configuration
     * @param midTick               whether a waiting runner may drain its region's intermediate tasks
     * @param utmiJobs              whether a parked runner may take jobs from an idle-safe source
     * @param sourcePresent         whether an idle-safe source is wired at all
     * @param idleWorkNanos         wall clock spent on idle work since start, in nanoseconds
     * @param jobsTaken             idle-safe jobs taken since start
     * @param midTickPasses         intermediate task passes run since start
     * @param preemptions           passes or jobs a due tick or a grant interrupted
     * @param addedTickLagMaxMillis worst lateness of a body that followed idle work, in milliseconds
     * @param shareOfIdlePercent    share of measured idle time spent on idle work, from 0 to 100
     * @param disabled              why the self-protection turned the path off
     */
    public record IdleWork(
        boolean enabled,
        boolean midTick,
        boolean utmiJobs,
        boolean sourcePresent,
        long idleWorkNanos,
        long jobsTaken,
        long midTickPasses,
        long preemptions,
        long addedTickLagMaxMillis,
        double shareOfIdlePercent,
        Disabled disabled
    ) {

        /**
         * Validates the record.
         *
         * @throws NullPointerException if {@code disabled} is {@code null}
         */
        public IdleWork {
            Objects.requireNonNull(disabled, "disabled");
        }

        /**
         * Why the idle path is not running.
         */
        public enum Disabled {

            /**
             * The path is allowed to run, or was never on.
             */
            NONE,

            /**
             * Idle work added more lateness than its budget, so it is off until the region is stable.
             */
            LAG_BUDGET
        }
    }
}
