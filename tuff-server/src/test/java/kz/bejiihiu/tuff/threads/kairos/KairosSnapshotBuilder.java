package kz.bejiihiu.tuff.threads.kairos;

import java.util.List;

/**
 * Builds a {@link KairosSnapshot} by hand, so command formatting can be tested without a fund.
 *
 * <p>The snapshot has one component per fact and the command report reads most of them, so a test that
 * only cares about the lease ranking should not have to spell out a supervisor decision. Every field
 * starts at the value a healthy idle fund would publish and a test overrides just the ones its branch
 * needs.
 */
final class KairosSnapshotBuilder {

    private boolean elastic = true;
    private int minThreads = 2;
    private int maxThreads = 4;
    private int liveThreads = 2;
    private int targetThreads = 2;
    private int queuedTicks;
    private int awaitingTicks;
    private int executingTicks;
    private int scheduledTicks;
    private int overdueTicks;
    private long oldestOverdueMillis;
    private double averageWaitMillis;
    private double longestWaitMillis;
    private long overdueThresholdMillis = 100L;
    private KairosSnapshot.SupervisorState supervisor = KairosSnapshot.SupervisorState.RUNNING;
    private KairosSnapshot.Decision lastDecision = KairosSnapshot.Decision.IDLE;
    private String lastDecisionDetail = "target 2";
    private long spawnCount;
    private long retireCount;
    private String budgetSource = "utmi budget (usable cores plus the deducted region threads)";
    private List<KairosSnapshot.Degraded> degradedReasons = List.of();
    private KairosSnapshot.LeaseWait leaseWait = new KairosSnapshot.LeaseWait(0L, 0L, 0L, 0L, 0L, List.of());
    private long affinityHits;
    private long stolenTicks;
    private long catchUpBursts;
    private long catchUpTicks;
    private KairosSnapshot.IdleWork idleWork = new KairosSnapshot.IdleWork(
        false, false, false, false, 0L, 0L, 0L, 0L, 0L, 0.0d,
        KairosSnapshot.IdleWork.Disabled.NONE
    );
    private long executedTicks;
    private long affinityMisses;
    private long overdueWarns;
    private long suppressedOverdueWarns;

    private KairosSnapshotBuilder() {
    }

    static KairosSnapshotBuilder aSnapshot() {
        return new KairosSnapshotBuilder();
    }

    KairosSnapshotBuilder minMax(final int min, final int max) {
        this.minThreads = min;
        this.maxThreads = max;
        return this;
    }

    KairosSnapshotBuilder elastic(final boolean value) {
        this.elastic = value;
        return this;
    }

    KairosSnapshotBuilder leaseWait(final KairosSnapshot.LeaseWait value) {
        this.leaseWait = value;
        return this;
    }

    KairosSnapshotBuilder degraded(final KairosSnapshot.Degraded... reasons) {
        this.degradedReasons = List.of(reasons);
        return this;
    }

    KairosSnapshotBuilder budgetSource(final String value) {
        this.budgetSource = value;
        return this;
    }

    KairosSnapshotBuilder counters(final long executed, final long hits, final long misses, final long steals) {
        this.executedTicks = executed;
        this.affinityHits = hits;
        this.affinityMisses = misses;
        this.stolenTicks = steals;
        return this;
    }

    KairosSnapshotBuilder warnings(final long written, final long suppressed) {
        this.overdueWarns = written;
        this.suppressedOverdueWarns = suppressed;
        return this;
    }

    KairosSnapshot build() {
        return new KairosSnapshot(
            this.elastic,
            this.minThreads,
            this.maxThreads,
            this.liveThreads,
            this.targetThreads,
            this.queuedTicks,
            this.awaitingTicks,
            this.executingTicks,
            this.scheduledTicks,
            this.overdueTicks,
            this.oldestOverdueMillis,
            this.averageWaitMillis,
            this.longestWaitMillis,
            this.overdueThresholdMillis,
            this.supervisor,
            this.lastDecision,
            this.lastDecisionDetail,
            this.spawnCount,
            this.retireCount,
            this.budgetSource,
            this.degradedReasons,
            this.leaseWait,
            this.affinityHits,
            this.stolenTicks,
            this.catchUpBursts,
            this.catchUpTicks,
            this.idleWork,
            this.executedTicks,
            this.affinityMisses,
            this.overdueWarns,
            this.suppressedOverdueWarns
        );
    }
}
