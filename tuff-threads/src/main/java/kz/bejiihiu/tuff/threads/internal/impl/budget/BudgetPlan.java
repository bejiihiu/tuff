package kz.bejiihiu.tuff.threads.internal.impl.budget;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.PoolId;

/**
 * The whole CPU ledger of one manager, computed once.
 *
 * <p>The arithmetic is deliberately boring and printable:
 *
 * <pre>{@code
 * ceiling = floor(availableProcessors * 0.8)
 * usable  = ceiling - region - chunk - io - reserve
 * }</pre>
 *
 * <p>{@code reserve} is the configured reserve floored at one, because a machine with no reserve left
 * for the kernel, GC and JIT does not become faster when a pool takes the last core. {@code critical}
 * is the part of the remainder held for the critical lanes that are not deducted as threads (the
 * network minimum, plus any region minimum the thread deduction did not cover): the shared pools
 * resolve against what is left after it, so their oversubscribed caps can never take it. When the
 * subtraction goes to zero or below, {@link #rawUsableCores()} keeps the negative number and
 * {@link #usableCores()} floors at one so the contract stays satisfied; the plan reports itself
 * degraded, which is the part an operator needs to see.
 *
 * @param availableProcessors  processors the JVM may use, at least one
 * @param ceilingCores         threads the server as a whole should stay under, at least one
 * @param regionCores          tick threads already claimed by the region scheduler
 * @param chunkCores           chunk workers already claimed
 * @param ioCores              chunk IO threads already claimed
 * @param reservedCores        cores kept for GC, JIT, the kernel and small pools, at least one
 * @param criticalReserveCores cores held for the guaranteed minimum of the critical lanes
 * @param rawUsableCores       the subtraction before the floor, may be zero or negative
 * @param usableCores          cores handed to the managed pools, at least one
 * @param pools                per pool budgets, in id order
 * @param degradedReasons      why the plan cannot be followed as written, empty when it can
 */
public record BudgetPlan(
    int availableProcessors,
    int ceilingCores,
    int regionCores,
    int chunkCores,
    int ioCores,
    int reservedCores,
    int criticalReserveCores,
    int rawUsableCores,
    int usableCores,
    List<PoolBudget> pools,
    List<String> degradedReasons
) {

    /**
     * Validates a plan.
     *
     * @throws IllegalArgumentException if a count is out of range
     * @throws NullPointerException     if a reference is {@code null}
     */
    public BudgetPlan {
        if (availableProcessors < 1) {
            throw new IllegalArgumentException(
                "availableProcessors must be at least 1, got " + availableProcessors
            );
        }
        if (ceilingCores < 1) {
            throw new IllegalArgumentException("ceilingCores must be at least 1, got " + ceilingCores);
        }
        if (regionCores < 0 || chunkCores < 0 || ioCores < 0) {
            throw new IllegalArgumentException(
                "thread deductions must not be negative, got "
                    + regionCores + ", " + chunkCores + ", " + ioCores
            );
        }
        if (reservedCores < 1) {
            throw new IllegalArgumentException("reservedCores must be at least 1, got " + reservedCores);
        }
        if (criticalReserveCores < 0) {
            throw new IllegalArgumentException(
                "criticalReserveCores must not be negative, got " + criticalReserveCores
            );
        }
        if (usableCores < 1) {
            throw new IllegalArgumentException("usableCores must be at least 1, got " + usableCores);
        }
        Objects.requireNonNull(pools, "pools");
        Objects.requireNonNull(degradedReasons, "degradedReasons");
        pools = List.copyOf(pools);
        degradedReasons = List.copyOf(degradedReasons);
    }

    /**
     * Looks up the budget of one pool.
     *
     * @param pool pool id
     * @return the budget, or an empty optional if the pool was not known when the plan was computed
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    public Optional<PoolBudget> pool(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        for (final PoolBudget budget : this.pools) {
            if (budget.pool().equals(pool)) {
                return Optional.of(budget);
            }
        }
        return Optional.empty();
    }

    /**
     * Checks whether some part of the ledger could not be honoured.
     *
     * @return {@code true} when there is at least one reason
     */
    public boolean degraded() {
        return !this.degradedReasons.isEmpty();
    }

    /**
     * Formats the ledger as one line for logs and commands.
     *
     * <p>Printing the full sum is the cheapest fix for the recurring "why does my pool have one core"
     * question: every term is visible instead of being hidden in a subtraction somewhere.
     *
     * @return a single line such as {@code cores 16: ceiling 12 = region 2 + chunk 4 + io 1 + reserve 1 + usable 4}
     */
    public String ledger() {
        final StringBuilder builder = new StringBuilder(96);
        builder.append("cores ").append(this.availableProcessors)
            .append(": ceiling ").append(this.ceilingCores)
            .append(" = region ").append(this.regionCores)
            .append(" + chunk ").append(this.chunkCores)
            .append(" + io ").append(this.ioCores)
            .append(" + reserve ").append(this.reservedCores);
        if (this.criticalReserveCores > 0) {
            builder.append(" + critical ").append(this.criticalReserveCores);
        }
        builder.append(" + usable ").append(this.rawUsableCores);
        if (this.rawUsableCores != this.usableCores) {
            builder.append(" (floored to ").append(this.usableCores).append(')');
        }
        return builder.toString();
    }
}
