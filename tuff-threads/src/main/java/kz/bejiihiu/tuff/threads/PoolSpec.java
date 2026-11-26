package kz.bejiihiu.tuff.threads;

import java.util.Objects;

/**
 * Registry entry of one execution pool.
 *
 * <p>A spec is the pool's identity card: who owns it, why it exists, how many threads it may have, how
 * deep its integration is and what CPU budget it declares. Registering a spec does not create,
 * resize, move or merge anything; it makes the pool visible and gives the resource controller a basis
 * for leases. The owning subsystem keeps full control over its queue, its ordering and its lifecycle.
 *
 * <p>Sizing uses the same {@code -1} convention as the rest of the server configuration:
 * {@code maxThreads == -1} means "no explicit ceiling, let the budget decide", while
 * {@code minThreads} is always a hard floor. A pool at {@link CoverageLevel#L2} or deeper must have a
 * positive floor, because "the manager creates the threads" and "the pool has no threads" cannot both
 * be true. Observe-only pools may declare zero.
 *
 * @param id         stable pool id
 * @param kind       category of work the pool performs
 * @param owner      subsystem that owns the pool, shown in commands and logs
 * @param purpose    one line description of what the pool is for
 * @param minThreads minimum number of threads, at least one when the manager creates them
 * @param maxThreads maximum number of threads, or {@code -1} for no explicit ceiling
 * @param coverage   integration level
 * @param budget     declared CPU budget
 */
public record PoolSpec(
    PoolId id,
    PoolKind kind,
    String owner,
    String purpose,
    int minThreads,
    int maxThreads,
    CoverageLevel coverage,
    ResourceBudget budget
) {

    /**
     * Validates a pool description.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if the owner or purpose is blank or the sizing is inconsistent
     */
    public PoolSpec {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(coverage, "coverage");
        Objects.requireNonNull(budget, "budget");
        if (owner.isBlank()) {
            throw new IllegalArgumentException("owner must not be blank");
        }
        if (purpose.isBlank()) {
            throw new IllegalArgumentException("purpose must not be blank");
        }
        if (minThreads < 0) {
            throw new IllegalArgumentException("minThreads must not be negative, got " + minThreads);
        }
        if (maxThreads != -1 && maxThreads < minThreads) {
            throw new IllegalArgumentException(
                "maxThreads must be -1 or at least minThreads, got " + maxThreads + " < " + minThreads
            );
        }
        if (coverage.managesThreads() && minThreads < 1) {
            throw new IllegalArgumentException(
                "a pool at " + coverage + " needs at least one managed thread, got minThreads " + minThreads
            );
        }
    }

    /**
     * Creates a description of a pool that is only observed.
     *
     * <p>The resulting spec is at {@link CoverageLevel#L1}, owns no threads from the manager's point of
     * view and declares an elastic budget it cannot actually use yet. It is the entry every subsystem
     * registers first; raising the coverage later is an explicit change, not a side effect of
     * registration.
     *
     * @param id      stable pool id
     * @param kind    category of work the pool performs
     * @param owner   subsystem that owns the pool
     * @param purpose one line description of the pool
     * @return the observe-only description
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if the owner or purpose is blank
     */
    public static PoolSpec observeOnly(
        final PoolId id,
        final PoolKind kind,
        final String owner,
        final String purpose
    ) {
        return new PoolSpec(id, kind, owner, purpose, 0, -1, CoverageLevel.L1, ResourceBudget.defaults());
    }

    /**
     * Checks whether the pool has no explicit thread ceiling.
     *
     * @return {@code true} if {@link #maxThreads()} is {@code -1}
     */
    public boolean elastic() {
        return this.maxThreads == -1;
    }

    /**
     * Checks whether the manager owns the pool's capacity.
     *
     * @return {@code true} when the coverage is {@link CoverageLevel#L3} or deeper
     */
    public boolean managesCapacity() {
        return this.coverage.managesCapacity();
    }

    /**
     * Returns a copy with another coverage level.
     *
     * <p>The copy keeps id, kind, owner, purpose, sizing and budget. Raising the level is a deliberate
     * integration step and is visible in the next snapshot; lowering it does not stop threads that
     * already exist, the owner still has to do that.
     *
     * @param newCoverage new integration level
     * @return the copy
     * @throws NullPointerException     if {@code newCoverage} is {@code null}
     * @throws IllegalArgumentException if the new level requires managed threads but
     *                                 {@link #minThreads()} is zero
     */
    public PoolSpec withCoverage(final CoverageLevel newCoverage) {
        return new PoolSpec(
            this.id,
            this.kind,
            this.owner,
            this.purpose,
            this.minThreads,
            this.maxThreads,
            newCoverage,
            this.budget
        );
    }
}
