package kz.bejiihiu.tuff.threads.internal.runtime;

import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetDeductions;

/**
 * The pool set a freshly started manager registers.
 *
 * <p>Every canonical pool gets an entry, because a pool that is not registered has no snapshot, no
 * saturation and no ownership answer. Registration is observe only unless the matching configuration
 * flag says otherwise, which is exactly the safe default the settings model already documents.
 *
 * <p>Compute is the exception that stays visible but shallow: this runtime cannot own the engine's
 * queue or its threads, so {@code compute} and {@code compute-virtual} are always registered at
 * {@link CoverageLevel#L1} here. The compute adapter re-registers them at L4 once the engine hands
 * its threads over; until that happens a configuration that asks for compute management is reported
 * as such instead of being pretended away.
 *
 * <p>Budgets are declared honestly. A pool the manager does not manage declares a zero core budget, so
 * the shared remainder is not silently split between pools that cannot use it yet; a pool that does
 * manage threads declares an elastic budget so its minimum stays satisfiable. The compute adapter
 * replaces its spec with a real budget when it registers at L4.
 */
final class DefaultPoolSpecs {

    private DefaultPoolSpecs() {
    }

    /**
     * Builds the default registrations for a configuration.
     *
     * @param settings effective configuration
     * @return the pool descriptions, in id order when sorted by the registry
     */
    static List<PoolSpec> collect(final ThreadManagementSettings settings) {
        final List<PoolSpec> specs = new ArrayList<>(9);
        specs.add(regionExecution(settings));
        specs.add(compute(settings));
        specs.add(computeVirtual());
        specs.add(chunkWorker(settings));
        specs.add(chunkIo(settings));
        specs.add(network(settings));
        specs.add(chat());
        specs.add(background());
        specs.add(service());
        return List.copyOf(specs);
    }

    /**
     * Derives the thread counts already claimed outside the managed budget.
     *
     * <p>Only the region count is known from this configuration model. Chunk and IO sizes live in the
     * platform config that this module cannot read, so they stay at zero and the plan treats the
     * remainder as if those lanes were not deducted yet - a known gap rather than an invented number.
     *
     * @param settings effective configuration
     * @return the deductions to hand to the controller
     */
    static BudgetDeductions deductions(final ThreadManagementSettings settings) {
        final int regionThreads = settings.regionExecution().threads() > 0
            ? settings.regionExecution().threads()
            : 0;
        return new BudgetDeductions(regionThreads, 0, 0);
    }

    private static PoolSpec regionExecution(final ThreadManagementSettings settings) {
        final ThreadManagementSettings.RegionExecution region = settings.regionExecution();
        final CoverageLevel coverage = region.managed() ? CoverageLevel.L2 : CoverageLevel.L1;
        final int minThreads = region.managed() ? 1 : 0;
        final int maxThreads = region.threads() > 0 ? region.threads() : -1;
        return new PoolSpec(
            PoolId.REGION_EXECUTION,
            PoolKind.REGION_EXECUTION,
            "folia",
            "region tick execution",
            minThreads,
            maxThreads,
            coverage,
            budgetFor(coverage)
        );
    }

    private static PoolSpec compute(final ThreadManagementSettings settings) {
        return new PoolSpec(
            PoolId.COMPUTE,
            PoolKind.COMPUTE,
            "tuff-compute",
            "platform workers of the async compute engine",
            0,
            settings.asyncCompute().maxThreads(),
            CoverageLevel.L1,
            budgetFor(CoverageLevel.L1)
        );
    }

    private static PoolSpec computeVirtual() {
        return new PoolSpec(
            PoolId.COMPUTE_VIRTUAL,
            PoolKind.VIRTUAL,
            "tuff-compute",
            "virtual lane of the async compute engine",
            0,
            -1,
            CoverageLevel.L1,
            budgetFor(CoverageLevel.L1)
        );
    }

    private static PoolSpec chunkWorker(final ThreadManagementSettings settings) {
        final boolean managed = settings.executionPools().chunkSystem().managed();
        final CoverageLevel coverage = managed ? CoverageLevel.L2 : CoverageLevel.L1;
        return new PoolSpec(
            PoolId.CHUNK_WORKER,
            PoolKind.CHUNK,
            "folia",
            "chunk generation and maintenance workers",
            managed ? 1 : 0,
            -1,
            coverage,
            budgetFor(coverage)
        );
    }

    private static PoolSpec chunkIo(final ThreadManagementSettings settings) {
        final boolean managed = settings.executionPools().chunkSystem().managed();
        final CoverageLevel coverage = managed ? CoverageLevel.L2 : CoverageLevel.L1;
        return new PoolSpec(
            PoolId.CHUNK_IO,
            PoolKind.IO,
            "folia",
            "chunk file IO workers",
            managed ? 1 : 0,
            -1,
            coverage,
            budgetFor(coverage)
        );
    }

    private static PoolSpec network(final ThreadManagementSettings settings) {
        final boolean managed = settings.executionPools().network().managed();
        final CoverageLevel coverage = managed ? CoverageLevel.L2 : CoverageLevel.L1;
        return new PoolSpec(
            PoolId.NETWORK,
            PoolKind.NETWORK,
            "netty",
            "network event loops",
            managed ? 1 : 0,
            -1,
            coverage,
            budgetFor(coverage)
        );
    }

    private static PoolSpec chat() {
        return new PoolSpec(
            PoolId.CHAT,
            PoolKind.SERVICE,
            "paper",
            "chat decoration executor",
            0,
            -1,
            CoverageLevel.L1,
            budgetFor(CoverageLevel.L1)
        );
    }

    private static PoolSpec background() {
        return new PoolSpec(
            PoolId.BACKGROUND,
            PoolKind.SERVICE,
            "paper",
            "generic background worker pool",
            0,
            -1,
            CoverageLevel.L1,
            budgetFor(CoverageLevel.L1)
        );
    }

    private static PoolSpec service() {
        return new PoolSpec(
            PoolId.SERVICE,
            PoolKind.SERVICE,
            "tuff",
            "short lived server services",
            0,
            -1,
            CoverageLevel.L1,
            budgetFor(CoverageLevel.L1)
        );
    }

    private static ResourceBudget budgetFor(final CoverageLevel coverage) {
        // a pool the manager does not create threads for claims nothing, so the shared remainder stays
        // with the pools that can actually use it today
        return coverage.managesThreads()
            ? ResourceBudget.elastic(1.0d, 0)
            : ResourceBudget.fixed(0, 0);
    }
}
