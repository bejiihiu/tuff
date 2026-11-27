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
 * <p>Lanes whose threads were already subtracted resolve their budget against their own thread count,
 * not against the ceiling: the region pool cannot hand out more leases than there are tick threads
 * running, and the same holds for chunk workers and chunk IO. Only network resolves against the ceiling,
 * because netty threads are not part of {@link BudgetDeductions} yet; that is a known gap, not a policy.
 *
 * <p>Pools that share what is left (compute, service, virtual) resolve against {@code usable}. Their
 * declared caps can add up to more than {@code usable} - a hundred elastic pools with share {@code 1.0}
 * is normal configuration - so the shared remainder is divided: declared minimums are funded first,
 * fixed budgets before elastic ones, and the space above them is split in proportion to each pool's
 * remaining headroom. An elastic minimum that no longer fits is reported as a shortfall instead of
 * being invented. Reserves are the guaranteed part of a budget and the only thing that may be
 * oversubscribed; that is reported too.
 *
 * <p>{@link #plan(List)} may be called once per planner. A second call is rejected instead of returning
 * a slightly different ledger, because two live ledgers are worse than a stale one. The controller
 * builds a fresh planner when the registry changed, so "once" is a property of one ledger, not of the
 * manager.
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
                "the budget plan is computed once per ledger; build another planner to rebuild it"
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

        final List<PoolSpec> ordered = specs.stream()
            .sorted(Comparator.comparing(PoolSpec::id))
            .toList();
        final Set<PoolId> seen = new HashSet<>();
        for (final PoolSpec spec : ordered) {
            if (!seen.add(spec.id())) {
                throw new IllegalArgumentException("duplicate pool spec for id '" + spec.id() + "'");
            }
        }

        // critical minimums that the thread deductions did not already pay for come out of the shared
        // remainder first: that is the part a compute burst can never take, even when its caps add up
        // to more than the machine has
        final int criticalReserve = criticalReserve(ordered, this.deductions);
        final int beforeCritical = ceiling
            - this.deductions.regionThreads()
            - this.deductions.chunkThreads()
            - this.deductions.ioThreads()
            - reserve;
        final int rawUsable = beforeCritical - criticalReserve;
        final int usable = Math.max(1, rawUsable);
        // what the pools that share the remainder may actually divide; zero is a legal answer and the
        // only honest one when the critical lanes hold everything that is left
        final int shared = Math.max(0, rawUsable);

        final List<PoolBudget> requested = new ArrayList<>(ordered.size());
        for (final PoolSpec spec : ordered) {
            requested.add(this.budgetFor(spec, ceiling, shared));
        }

        final List<String> reasons = new ArrayList<>();
        final int reservedTotal = requested.stream()
            .filter(budget -> !budget.critical())
            .mapToInt(PoolBudget::reservedCores)
            .sum();
        if (reservedTotal > shared) {
            reasons.add(
                "pool reserves reach " + reservedTotal + " cores but only " + shared
                    + " are usable; not overcommitting them"
            );
        }

        final int sharedDemand = requested.stream()
            .filter(BudgetPlanner::sharesUsable)
            .mapToInt(PoolBudget::maxCores)
            .sum();
        final List<PoolBudget> budgets;
        if (sharedDemand > shared) {
            reasons.add(
                "shared pool caps reach " + sharedDemand + " cores but only " + shared
                    + " are usable; funding declared minimums first, then dividing the remainder"
            );
            budgets = divideUsable(requested, shared);
        } else {
            budgets = requested;
        }
        for (final PoolBudget budget : budgets) {
            if (!budget.satisfied()) {
                reasons.add(budget.degradedReason());
            }
        }
        final int criticalCapacity = budgets.stream()
            .filter(PoolBudget::critical)
            .mapToInt(PoolBudget::capacity)
            .sum();
        if (criticalCapacity > usable) {
            reasons.add(
                "critical pools may lease " + criticalCapacity + " cores, above the " + usable
                    + " usable; the excess is best effort and is reported, not reserved twice"
            );
        }

        if (beforeCritical <= 0) {
            reasons.add(
                "no headroom: region " + this.deductions.regionThreads()
                    + " + chunk " + this.deductions.chunkThreads()
                    + " + io " + this.deductions.ioThreads()
                    + " + reserve " + reserve
                    + " leaves " + beforeCritical + " of the " + ceiling + " thread ceiling"
            );
        } else if (criticalReserve > 0 && rawUsable <= 0) {
            reasons.add(
                "critical pools hold " + criticalReserve + " of the " + beforeCritical
                    + " cores left by the deductions, so the non critical pools get none"
            );
        }

        return new BudgetPlan(
            available,
            ceiling,
            this.deductions.regionThreads(),
            this.deductions.chunkThreads(),
            this.deductions.ioThreads(),
            reserve,
            criticalReserve,
            rawUsable,
            usable,
            budgets,
            reasons
        );
    }

    /**
     * Sums the guaranteed minimum of the critical lanes that is not already deducted as threads.
     *
     * <p>Region tick threads are subtracted from the headroom as threads, so only a declared minimum
     * above that count needs cores of its own. Netty threads are not deducted anywhere yet, which makes
     * the whole network minimum a reserve. The guarantee of one pool is the larger of its declared
     * minimum and its declared reserve, because both are promises the pool already makes. Anything a
     * critical pool asks for above its guarantee is best effort and is not held back from the others - a
     * guarantee that grows with the request is not a guarantee.
     *
     * @param specs      registered pools, in id order
     * @param deductions threads already claimed outside the managed budget
     * @return cores to keep out of the shared remainder, at least zero
     */
    private static int criticalReserve(final List<PoolSpec> specs, final BudgetDeductions deductions) {
        int reserve = 0;
        for (final PoolSpec spec : specs) {
            final int guarantee = Math.max(spec.minThreads(), spec.budget().reservedCores());
            if (spec.kind() == PoolKind.REGION_EXECUTION) {
                reserve += Math.max(0, guarantee - deductions.regionThreads());
            } else if (spec.kind() == PoolKind.NETWORK) {
                reserve += guarantee;
            }
        }
        return reserve;
    }

    /**
     * Resolves one pool's declared budget against the right base.
     *
     * @param spec    pool description
     * @param ceiling thread ceiling of the whole server
     * @param shared  cores left for the pools that share the remainder, may be zero
     * @return the pool budget
     */
    private PoolBudget budgetFor(final PoolSpec spec, final int ceiling, final int shared) {
        final int base = this.baseFor(spec.kind(), ceiling, shared);
        final int resolved = spec.budget().resolveCores(base);
        final int capped = spec.maxThreads() == -1 ? resolved : Math.min(resolved, spec.maxThreads());
        final boolean critical = spec.kind() == PoolKind.REGION_EXECUTION || spec.kind() == PoolKind.NETWORK;
        final int capacity = critical ? Math.max(capped, spec.minThreads()) : clampToShared(capped, shared);
        final int reserved = Math.min(spec.budget().reservedCores(), capacity);
        return new PoolBudget(
            spec.id(), spec.kind(), spec.minThreads(), capped, reserved, spec.budget().isElastic()
        );
    }

    /**
     * Keeps a shared pool from claiming a remainder that is not there.
     *
     * <p>An elastic budget resolves to at least one core so a registered pool can make progress; when
     * the critical reserve ate the whole remainder that one core would be an invented core, exactly the
     * overcommit the ledger exists to prevent. Zero permits is a legal answer for a pool, and the plan
     * says why.
     *
     * @param capacity resolved capacity
     * @param shared   cores actually left for the shared pools
     * @return the capacity the pool may hold
     */
    private static int clampToShared(final int capacity, final int shared) {
        return Math.max(0, Math.min(capacity, shared));
    }

    /**
     * Returns the core count a pool's budget is resolved against.
     *
     * <p>Lanes whose threads were subtracted from {@code usable} resolve against their own population:
     * a lease for a tick is only useful while a tick thread exists to use it. A zero population still
     * resolves against one, because {@code resolveCores} rejects a zero base and an unconfigured
     * deduction means "we do not know", not "no threads ever".
     *
     * @param kind    pool kind
     * @param ceiling thread ceiling of the whole server
     * @param shared  cores left for the pools that share the remainder, may be zero
     * @return the base, at least one
     */
    private int baseFor(final PoolKind kind, final int ceiling, final int shared) {
        return switch (kind) {
            case REGION_EXECUTION -> Math.max(1, this.deductions.regionThreads());
            case CHUNK -> Math.max(1, this.deductions.chunkThreads());
            case IO -> Math.max(1, this.deductions.ioThreads());
            // netty threads are not deducted yet, so the ceiling is the only honest base for now
            case NETWORK -> ceiling;
            case COMPUTE, SERVICE, VIRTUAL -> Math.max(1, shared);
        };
    }

    /**
     * Checks whether a pool draws its permits from the shared remainder.
     *
     * @param budget pool budget
     * @return {@code true} for compute, service and virtual lanes
     */
    private static boolean sharesUsable(final PoolBudget budget) {
        return switch (budget.kind()) {
            case COMPUTE, SERVICE, VIRTUAL -> true;
            case REGION_EXECUTION, CHUNK, IO, NETWORK -> false;
        };
    }

    /**
     * Splits {@code usable} between the shared pools, funding declared minimums first.
     *
     * <p>The order is policy, not taste. Absolute (fixed) budgets come before elastic ones, because a
     * fixed budget named a number while an elastic one asked for "whatever is left". Inside each group
     * the id order keeps the result deterministic. That way a pool with a real thread and a one core
     * claim (the service refresher, the virtual lane's pump) keeps its core even on a machine where the
     * elastic pool's declared minimum no longer fits; the elastic pool is the one that reports the
     * shortfall. What is left after the minimums is divided in proportion to each pool's remaining
     * headroom, largest remainder first, id order as the tie breaker, so the sum never exceeds the
     * remainder.
     *
     * @param budgets requested budgets in id order
     * @param usable  cores available to the shared pools, may be zero
     * @return budgets with capped shared capacities
     */
    private static List<PoolBudget> divideUsable(final List<PoolBudget> budgets, final int usable) {
        final int[] allocated = new int[budgets.size()];
        int remaining = Math.max(0, usable);
        final List<Integer> order = new ArrayList<>(budgets.size());
        for (int index = 0; index < budgets.size(); index++) {
            if (sharesUsable(budgets.get(index))) {
                order.add(index);
            }
        }
        order.sort(Comparator
            .comparingInt((Integer index) -> budgets.get(index).elastic() ? 1 : 0)
            .thenComparing(index -> budgets.get(index).pool()));
        for (final int index : order) {
            final PoolBudget budget = budgets.get(index);
            final int floor = Math.min(budget.minCores(), budget.maxCores());
            final int funded = Math.min(floor, remaining);
            allocated[index] = funded;
            remaining -= funded;
        }
        if (remaining > 0) {
            divideHeadroom(budgets, allocated, remaining);
        }

        final List<PoolBudget> capped = new ArrayList<>(budgets.size());
        for (int index = 0; index < budgets.size(); index++) {
            final PoolBudget budget = budgets.get(index);
            if (!sharesUsable(budget)) {
                capped.add(budget);
                continue;
            }
            final int capacity = Math.min(budget.maxCores(), allocated[index]);
            capped.add(new PoolBudget(
                budget.pool(),
                budget.kind(),
                budget.minCores(),
                capacity,
                Math.min(budget.reservedCores(), capacity),
                budget.elastic()
            ));
        }
        return List.copyOf(capped);
    }

    /**
     * Spreads what the minimums did not use in proportion to the pools' remaining headroom.
     *
     * <p>Largest remainder first, id order as the tie breaker: the same rule the ledger used before,
     * just applied to the space above the funded minimums instead of the whole cap.
     *
     * @param budgets   requested budgets in id order
     * @param allocated per pool allocation so far, updated in place
     * @param remaining cores still unassigned, at least one
     */
    private static void divideHeadroom(
        final List<PoolBudget> budgets,
        final int[] allocated,
        final int remaining
    ) {
        int headroomTotal = 0;
        final long[] remainder = new long[budgets.size()];
        for (int index = 0; index < budgets.size(); index++) {
            final PoolBudget budget = budgets.get(index);
            if (!sharesUsable(budget)) {
                continue;
            }
            headroomTotal += Math.max(0, budget.maxCores() - allocated[index]);
        }
        if (headroomTotal <= 0) {
            return;
        }
        int assigned = 0;
        for (int index = 0; index < budgets.size(); index++) {
            final PoolBudget budget = budgets.get(index);
            if (!sharesUsable(budget)) {
                continue;
            }
            final int headroom = Math.max(0, budget.maxCores() - allocated[index]);
            final long scaled = (long) remaining * headroom;
            final int share = (int) (scaled / headroomTotal);
            allocated[index] += share;
            remainder[index] = scaled % headroomTotal;
            assigned += share;
        }
        int leftover = remaining - assigned;
        while (leftover > 0) {
            int best = -1;
            for (int index = 0; index < budgets.size(); index++) {
                final PoolBudget budget = budgets.get(index);
                if (!sharesUsable(budget) || allocated[index] >= budget.maxCores()) {
                    continue;
                }
                if (best < 0
                    || remainder[index] > remainder[best]
                    || (remainder[index] == remainder[best]
                        && budget.pool().compareTo(budgets.get(best).pool()) < 0)) {
                    best = index;
                }
            }
            if (best < 0) {
                break;
            }
            allocated[best]++;
            // this pool already got its extra core, so the next pass must consider the others
            remainder[best] = -1L;
            leftover--;
        }
    }
}
