package kz.bejiihiu.tuff.threads.internal.impl.budget;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;

/**
 * Divides the processor headroom into per pool budgets.
 *
 * <p>The formula is the one the regioniser documentation recommends and the compute engine already
 * used, kept in one place this time:
 *
 * <pre>{@code
 * ceiling = floor(availableProcessors * 0.8)
 * usable  = ceiling - regionThreads - chunkThreads - ioThreads - max(1, configuredReserve)
 * }</pre>
 *
 * <p>Pools whose threads were already subtracted resolve their declared budget against the ceiling, so
 * a fixed budget keeps its value and an elastic one can never claim more than the machine has. Pools
 * that share what is left resolve against {@code usable}. Reserves are the guaranteed part of a budget
 * and are the only thing that can be oversubscribed: elastic shares are caps, and a hundred pools with
 * share {@code 1.0} is normal, not a conflict.
 *
 * <p>{@link #plan(List)} may be called once. A second call is rejected instead of returning a slightly
 * different ledger, because two live ledgers are worse than a stale one.
 */
public final class BudgetPlanner {

    /**
     * Share of the processors the server as a whole should stay under.
     */
    public static final double USABLE_CORE_FRACTION = 0.8d;

    /**
     * Smallest reserve that is ever honoured, whatever the configuration says.
     */
    public static final int MINIMUM_RESERVE_CORES = 1;

    private final TopologyView topology;
    private final int configuredReserve;
    private final BudgetDeductions deductions;
    private final AtomicReference<BudgetPlan> computed = new AtomicReference<>();

    /**
     * Creates a planner.
     *
     * @param topology   processor topology to size against
     * @param resources  resource settings, providing the configured reserve
     * @param deductions threads already claimed outside the managed budget
     * @throws NullPointerException if a reference is {@code null}
     */
    public BudgetPlanner(
        final TopologyView topology,
        final ThreadManagementSettings.Resources resources,
        final BudgetDeductions deductions
    ) {
        this.topology = Objects.requireNonNull(topology, "topology");
        this.deductions = Objects.requireNonNull(deductions, "deductions");
        this.configuredReserve = Objects.requireNonNull(resources, "resources").reserveCores();
    }

    /**
     * Computes the plan for a set of pools.
     *
     * <p>Only the first call succeeds. Pools are processed in id order so two runs with the same input
     * produce the same ledger, which is what makes the numbers comparable across restarts.
     *
     * @param specs registered pools
     * @return the plan
     * @throws NullPointerException     if {@code specs} is {@code null}
     * @throws IllegalArgumentException if two specs share an id
     * @throws IllegalStateException    if the plan was already computed
     */
    public BudgetPlan plan(final List<PoolSpec> specs) {
        Objects.requireNonNull(specs, "specs");
        final BudgetPlan built = this.build(specs);
        if (!this.computed.compareAndSet(null, built)) {
            throw new IllegalStateException(
                "the budget plan is computed once per manager; a second ledger would be a second source of truth"
            );
        }
        return built;
    }

    /**
     * Returns the plan if it was already computed.
     *
     * @return the plan, or an empty optional before the first {@link #plan(List)} call
     */
    public Optional<BudgetPlan> current() {
        return Optional.ofNullable(this.computed.get());
    }

    /**
     * Returns the reserve actually applied.
     *
     * @return the configured reserve floored at {@link #MINIMUM_RESERVE_CORES}
     */
    public int reserveCores() {
        return Math.max(MINIMUM_RESERVE_CORES, this.configuredReserve);
    }

    /**
     * Builds the plan.
     *
     * @param specs registered pools
     * @return the plan
     * @throws IllegalArgumentException if two specs share an id
     */
    private BudgetPlan build(final List<PoolSpec> specs) {
        final int available = Math.max(1, this.topology.availableProcessors());
        final int ceiling = Math.max(1, (int) Math.floor(available * USABLE_CORE_FRACTION));
        final int reserve = this.reserveCores();
        final int rawUsable = ceiling
            - this.deductions.regionThreads()
            - this.deductions.chunkThreads()
            - this.deductions.ioThreads()
            - reserve;
        final int usable = Math.max(1, rawUsable);

        final List<PoolSpec> ordered = specs.stream()
            .sorted(Comparator.comparing(PoolSpec::id))
            .toList();
        final Set<PoolId> seen = new HashSet<>();
        final List<PoolBudget> budgets = new ArrayList<>(ordered.size());
        final List<String> reasons = new ArrayList<>();
        int reservedTotal = 0;

        for (final PoolSpec spec : ordered) {
            if (!seen.add(spec.id())) {
                throw new IllegalArgumentException("duplicate pool spec for id '" + spec.id() + "'");
            }
            final PoolBudget budget = this.budgetFor(spec, ceiling, usable);
            budgets.add(budget);
            if (!budget.satisfied()) {
                reasons.add(budget.degradedReason());
            }
            if (!budget.critical()) {
                reservedTotal += budget.reservedCores();
            }
        }

        if (rawUsable <= 0) {
            reasons.add(
                "no headroom: region " + this.deductions.regionThreads()
                    + " + chunk " + this.deductions.chunkThreads()
                    + " + io " + this.deductions.ioThreads()
                    + " + reserve " + reserve
                    + " leaves " + rawUsable + " of the " + ceiling + " thread ceiling"
            );
        }
        if (reservedTotal > usable) {
            reasons.add(
                "pool reserves reach " + reservedTotal + " cores but only " + usable
                    + " are usable; not overcommitting them"
            );
        }

        return new BudgetPlan(
            available,
            ceiling,
            this.deductions.regionThreads(),
            this.deductions.chunkThreads(),
            this.deductions.ioThreads(),
            reserve,
            rawUsable,
            usable,
            budgets,
            reasons
        );
    }

    /**
     * Resolves one pool's declared budget against the right base.
     *
     * @param spec    pool description
     * @param ceiling thread ceiling of the whole server
     * @param usable  cores left for the pools that share the remainder
     * @return the pool budget
     */
    private PoolBudget budgetFor(final PoolSpec spec, final int ceiling, final int usable) {
        final int base = switch (spec.kind()) {
            case REGION_EXECUTION, CHUNK, IO, NETWORK -> ceiling;
            case COMPUTE, SERVICE, VIRTUAL -> usable;
        };
        final int resolved = spec.budget().resolveCores(base);
        final int capped = spec.maxThreads() == -1 ? resolved : Math.min(resolved, spec.maxThreads());
        final boolean critical = spec.kind() == PoolKind.REGION_EXECUTION || spec.kind() == PoolKind.NETWORK;
        final int capacity = critical ? Math.max(capped, spec.minThreads()) : capped;
        final int reserved = Math.min(spec.budget().reservedCores(), capacity);
        return new PoolBudget(spec.id(), spec.kind(), spec.minThreads(), capped, reserved);
    }
}
