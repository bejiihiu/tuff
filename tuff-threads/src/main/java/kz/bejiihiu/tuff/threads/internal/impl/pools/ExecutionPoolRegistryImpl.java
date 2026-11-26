package kz.bejiihiu.tuff.threads.internal.impl.pools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;

/**
 * Default pool registry: one concurrent map of specs, one write lock, no threads.
 *
 * <p>Reads go straight to the map and copy what they need, so a command or a sampler never waits for a
 * registration. Writes take a private lock, because the contract asks for registration and removal to
 * be serialised and because the stop flag must be decided together with the write: a register that
 * passes the flag check and then lands after {@link #markStopped()} would resurrect a pool in a manager
 * that is already shutting down.
 *
 * <p>The rules that are worth restating, because they are the whole point of the class:
 * <ul>
 *   <li>same id and same kind replaces the spec - that is how coverage and sizing move between phases;
 *   <li>same id and another kind is refused - it would reassign metrics and leases of a subsystem to a
 *       different one;
 *   <li>{@link CoverageLevel#L0} is refused - L0 means "outside the manager on purpose" and such a pool
 *       has no registry entry by definition;
 *   <li>after {@link #markStopped()} nothing new is registered, but reads and removals keep working, so
 *       shutdown can still tear the bookkeeping down in whatever order it likes.
 * </ul>
 */
public final class ExecutionPoolRegistryImpl implements ExecutionPoolRegistry {

    private final ConcurrentHashMap<PoolId, PoolSpec> specs = new ConcurrentHashMap<>();
    private final Object writeLock = new Object();
    private volatile boolean stopped;

    /**
     * Creates an empty registry.
     */
    public ExecutionPoolRegistryImpl() {
    }

    @Override
    public PoolSpec register(final PoolSpec spec) {
        Objects.requireNonNull(spec, "spec");
        if (spec.coverage() == CoverageLevel.L0) {
            throw new IllegalArgumentException(
                "pool '" + spec.id() + "' declares L0: L0 pools live outside the manager on purpose and "
                    + "have no registry entry"
            );
        }
        synchronized (this.writeLock) {
            if (this.stopped) {
                throw new IllegalStateException(
                    "pool registry is stopped, cannot register '" + spec.id() + "'"
                );
            }
            final PoolSpec existing = this.specs.get(spec.id());
            if (existing != null && existing.kind() != spec.kind()) {
                throw new IllegalStateException(
                    "pool '" + spec.id() + "' is registered as " + existing.kind()
                        + " and cannot be re-registered as " + spec.kind()
                );
            }
            this.specs.put(spec.id(), spec);
            return spec;
        }
    }

    @Override
    public boolean unregister(final PoolId id) {
        Objects.requireNonNull(id, "id");
        synchronized (this.writeLock) {
            return this.specs.remove(id) != null;
        }
    }

    @Override
    public Optional<PoolSpec> spec(final PoolId id) {
        Objects.requireNonNull(id, "id");
        return Optional.ofNullable(this.specs.get(id));
    }

    @Override
    public boolean contains(final PoolId id) {
        Objects.requireNonNull(id, "id");
        return this.specs.containsKey(id);
    }

    @Override
    public Set<PoolId> ids() {
        // a fresh tree set gives both the natural order and a snapshot the caller cannot mutate
        return Collections.unmodifiableSet(new TreeSet<>(this.specs.keySet()));
    }

    @Override
    public List<PoolSpec> specs() {
        final List<PoolSpec> snapshot = new ArrayList<>(this.specs.values());
        snapshot.sort((left, right) -> left.id().compareTo(right.id()));
        return List.copyOf(snapshot);
    }

    @Override
    public List<PoolSpec> specs(final PoolKind kind) {
        Objects.requireNonNull(kind, "kind");
        final List<PoolSpec> matching = new ArrayList<>();
        for (final PoolSpec spec : this.specs.values()) {
            if (spec.kind() == kind) {
                matching.add(spec);
            }
        }
        matching.sort((left, right) -> left.id().compareTo(right.id()));
        return List.copyOf(matching);
    }

    /**
     * Refuses further registrations while keeping reads and removals available.
     *
     * <p>Called by the runtime when shutdown starts, before the factory and the thread registry are
     * stopped. A late registration is a bug: a pool that appears while the manager is going down could
     * never be scheduled or measured.
     */
    public void markStopped() {
        synchronized (this.writeLock) {
            this.stopped = true;
        }
    }

    /**
     * Checks whether registrations are refused.
     *
     * @return {@code true} after {@link #markStopped()}
     */
    public boolean isStopped() {
        return this.stopped;
    }
}
