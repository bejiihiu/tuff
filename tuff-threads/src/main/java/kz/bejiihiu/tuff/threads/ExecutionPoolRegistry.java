package kz.bejiihiu.tuff.threads;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Central registry of execution pools.
 *
 * <p>Every pool the server knows about is registered here once, with its owner, its purpose, its
 * sizing and its declared integration level. Registration is bookkeeping, not adoption: it does not
 * create, resize, merge or otherwise touch the pool's threads. A subsystem that registers a pool keeps
 * owning its queue, its ordering guarantees and its shutdown; UTMI only gets the right to observe it,
 * and later to supply its factory or budget if the coverage level says so.
 *
 * <p>All methods are thread safe and may be called during bootstrap, reload and shutdown. Reads are
 * cheap and are expected from command handlers; registration and removal are rare and are serialised
 * inside the implementation. Every method that returns a collection returns an immutable snapshot, so
 * callers never hold a live view that changes under them.
 */
public interface ExecutionPoolRegistry {

    /**
     * Registers a pool, or replaces the spec of an already registered one.
     *
     * <p>Registration is idempotent by id, which is what makes reload and repeated bootstrap safe. If
     * the id is new, the spec is stored. If the id exists with the same {@link PoolSpec#kind()}, the
     * stored spec is replaced: this is how coverage, sizing and budget move from one phase to the next.
     * If the id exists with a different kind, the call fails, because that would silently reassign an
     * existing pool's history and metrics to a different subsystem.
     *
     * <p>Replacing a spec does not create or stop threads. Lowering the coverage does not stop threads
     * that already exist; the pool owner remains responsible for its own lifecycle.
     *
     * @param spec pool description
     * @return the stored spec, equal to {@code spec}
     * @throws NullPointerException     if {@code spec} is {@code null}
     * @throws IllegalArgumentException if the spec declares {@link CoverageLevel#L0}, which by
     *                                 definition has no registry entry
     * @throws IllegalStateException    if the id is registered with a different kind or the manager has
     *                                  already stopped
     */
    PoolSpec register(PoolSpec spec);

    /**
     * Removes a pool from the registry.
     *
     * <p>The pool's threads are not stopped and its queue is not touched. Leases for the id start to
     * fail and probes are detached, so the pool disappears from snapshots. Its measured history is
     * kept until the metrics layer's retention expires, because deleting it on unregister would make
     * reload look like the pool was never there.
     *
     * @param id pool id
     * @return {@code true} if a spec with that id was registered
     * @throws NullPointerException if {@code id} is {@code null}
     */
    boolean unregister(PoolId id);

    /**
     * Looks up a registered pool.
     *
     * @param id pool id
     * @return the stored spec, or an empty optional if it is not registered
     * @throws NullPointerException if {@code id} is {@code null}
     */
    Optional<PoolSpec> spec(PoolId id);

    /**
     * Checks whether a pool id is registered.
     *
     * @param id pool id
     * @return {@code true} if a spec with that id is present
     * @throws NullPointerException if {@code id} is {@code null}
     */
    boolean contains(PoolId id);

    /**
     * Returns all registered ids.
     *
     * @return an immutable set of ids, in natural order
     */
    Set<PoolId> ids();

    /**
     * Returns all registered specs.
     *
     * @return an immutable list of specs, in id order
     */
    List<PoolSpec> specs();

    /**
     * Returns the registered specs of one kind.
     *
     * @param kind kind to filter by
     * @return an immutable list of matching specs, in id order
     * @throws NullPointerException if {@code kind} is {@code null}
     */
    List<PoolSpec> specs(PoolKind kind);
}
