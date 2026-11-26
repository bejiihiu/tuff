package kz.bejiihiu.tuff.threads.internal.impl.pools;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbeRegistry;

/**
 * Default probe registry: one concurrent map, keyed by the pool the probe observes.
 *
 * <p>A probe is attached by the pool owner, after the spec is registered. Attaching twice replaces the
 * previous probe, which makes reload idempotent; detaching is a plain remove and leaves the spec alone,
 * because losing a thermometer does not mean the pool stopped existing.
 *
 * <p>Unregistering a pool does not go through this class, so {@link #attached()} also drops probes whose
 * pool is no longer registered. That keeps the registry contract honest ("unregister detaches probes")
 * without a listener between the two maps, and it prevents a stale probe from resurfacing if the same id
 * is registered again later.
 *
 * <p>{@link #attached()} exists because the frozen SPI can only attach and detach, while the metrics
 * layer needs to enumerate what is attached. It returns a snapshot sorted by pool id, so the sampler and
 * the command output agree on the order without sorting twice.
 */
public final class PoolProbeRegistryImpl implements PoolProbeRegistry {

    private final ExecutionPoolRegistry pools;
    private final ConcurrentHashMap<PoolId, PoolProbe> probes = new ConcurrentHashMap<>();
    private volatile boolean stopped;

    /**
     * Creates a probe registry bound to a pool registry.
     *
     * @param pools pool registry used to check that a probe belongs to a known pool
     * @throws NullPointerException if {@code pools} is {@code null}
     */
    public PoolProbeRegistryImpl(final ExecutionPoolRegistry pools) {
        this.pools = Objects.requireNonNull(pools, "pools");
    }

    @Override
    public void attach(final PoolProbe probe) {
        Objects.requireNonNull(probe, "probe");
        if (this.stopped) {
            throw new IllegalStateException(
                "pool probe registry is stopped, cannot attach a probe for '" + probe.pool() + "'"
            );
        }
        if (!this.pools.contains(probe.pool())) {
            throw new IllegalStateException(
                "pool '" + probe.pool() + "' is not registered, attach the spec first"
            );
        }
        this.probes.put(probe.pool(), probe);
    }

    @Override
    public boolean detach(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        return this.probes.remove(pool) != null;
    }

    /**
     * Returns the probes attached right now.
     *
     * <p>A snapshot, sorted by pool id. Used by the runtime to feed {@code PoolProbeSource}; the sampler
     * calls it once per pass and must never see a live view.
     *
     * @return an immutable list of probes, possibly empty
     */
    public List<PoolProbe> attached() {
        // drop probes of pools that were unregistered behind our back, so the sampler never sees a ghost
        this.probes.entrySet().removeIf(entry -> !this.pools.contains(entry.getKey()));
        final List<PoolProbe> snapshot = new ArrayList<>(this.probes.values());
        snapshot.sort((left, right) -> left.pool().compareTo(right.pool()));
        return List.copyOf(snapshot);
    }

    /**
     * Refuses further attachments while keeping detachments available.
     *
     * <p>Called by the runtime when shutdown starts. Detaching stays legal so a pool owner finishing its
     * own shutdown does not have to guess whether the manager is already down.
     */
    public void markStopped() {
        this.stopped = true;
    }

    /**
     * Checks whether attachments are refused.
     *
     * @return {@code true} after {@link #markStopped()}
     */
    public boolean isStopped() {
        return this.stopped;
    }
}
