package kz.bejiihiu.tuff.threads.internal.impl.budget;

import java.util.Objects;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;

/**
 * What one pool is allowed to use after the ledger was divided.
 *
 * <p>Two numbers matter and they are kept apart on purpose. {@link #maxCores()} is the ceiling the
 * arithmetic allows; {@link #minCores()} is what the pool declared it needs. When the ceiling is below
 * the minimum the plan says so instead of quietly raising the ceiling, because a budget that grows
 * itself to fit a minimum is not a budget.
 *
 * <p>Critical pools are the one exception, and it is an explicit policy, not an accident: region ticks
 * and network event loops are the lanes the whole server depends on, so they keep their declared
 * minimum even when that overshoots the arithmetic, and the overshoot is reported as an overcommit.
 *
 * @param pool          pool id
 * @param kind          pool kind
 * @param minCores      declared minimum in cores, zero for an observe-only pool
 * @param maxCores      budget ceiling in cores, never silently raised to the minimum
 * @param reservedCores guaranteed share inside the ceiling, clamped so it can never exceed it
 * @param elastic       whether the pool asked for a share instead of an absolute count
 */
public record PoolBudget(PoolId pool, PoolKind kind, int minCores, int maxCores, int reservedCores, boolean elastic) {

    /**
     * Validates a pool budget.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a count is negative
     */
    public PoolBudget {
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(kind, "kind");
        if (minCores < 0) {
            throw new IllegalArgumentException("minCores must not be negative, got " + minCores);
        }
        if (maxCores < 0) {
            throw new IllegalArgumentException("maxCores must not be negative, got " + maxCores);
        }
        if (reservedCores < 0) {
            throw new IllegalArgumentException("reservedCores must not be negative, got " + reservedCores);
        }
    }

    /**
     * Checks whether the pool is one the server cannot run without.
     *
     * @return {@code true} for region execution and network pools
     */
    public boolean critical() {
        return this.kind == PoolKind.REGION_EXECUTION || this.kind == PoolKind.NETWORK;
    }

    /**
     * Checks whether the ceiling covered the declared minimum.
     *
     * @return {@code true} when {@link #maxCores()} is at least {@link #minCores()}
     */
    public boolean satisfied() {
        return this.maxCores >= this.minCores;
    }

    /**
     * Returns the number of permits the pool actually gets.
     *
     * <p>Equal to the ceiling for a normal pool. For a critical pool it is the larger of the ceiling and
     * the minimum, so a bad arithmetic result can never take the tick threads or the event loops away.
     *
     * @return permits, never negative
     */
    public int capacity() {
        return this.critical() ? Math.max(this.maxCores, this.minCores) : this.maxCores;
    }

    /**
     * Returns how far {@link #capacity()} exceeds the budget ceiling.
     *
     * @return cores above the ceiling, zero when the budget was enough
     */
    public int overcommit() {
        return this.capacity() - this.maxCores;
    }

    /**
     * Explains why the pool is below its declared minimum.
     *
     * <p>Empty when the pool is satisfied, so a caller can print the reason list without filtering. The
     * text names the pool, both numbers and the decision that was taken, which is the whole point of
     * degradation being a first class state.
     *
     * @return a one line reason, or an empty string when nothing is wrong
     */
    public String degradedReason() {
        if (this.satisfied()) {
            return "";
        }
        final String base = "pool '" + this.pool + "' needs at least " + this.minCores
            + " cores but the budget allows " + this.maxCores;
        if (this.critical()) {
            return base + "; keeping the critical minimum and overcommitting by " + this.overcommit();
        }
        return base + "; not expanding past the budget";
    }
}
