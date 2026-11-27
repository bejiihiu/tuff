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
    long catchUpTicks
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
}
