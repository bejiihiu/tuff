package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;

/**
 * {@link ResourceMetrics} over a {@link ResourceSampler}.
 *
 * <p>Every accessor answers from the latest snapshot, so one call site sees one moment and two accessors
 * never disagree. The sampler does the work; this class only chooses what to publish. Two different
 * "nothing to show" cases stay apart: collection switched off publishes a structural snapshot (registered
 * pools with unmeasured values, no probes, no rates) and keeps {@link #enabled()} {@code false}, while a
 * stopped manager publishes the last snapshot that was taken, because {@code UnifiedThreadManager}
 * promises the last known values after {@code STOPPED}.
 *
 * <p>Cost: the fast accessors are volatile field reads. {@link #snapshot()} is the only method that may
 * take a pass, and only when the interval elapsed; it never waits for another pass in flight.
 */
public final class ResourceMetricsImpl implements ResourceMetrics {

    private final ResourceSampler sampler;

    /**
     * Creates the view.
     *
     * @param sampler sampler that owns the measured values
     * @throws NullPointerException if {@code sampler} is {@code null}
     */
    public ResourceMetricsImpl(final ResourceSampler sampler) {
        this.sampler = Objects.requireNonNull(sampler, "sampler");
    }

    /**
     * Returns the sampler behind this view, for a driver that wants to force or throttle passes.
     *
     * @return the sampler
     */
    public ResourceSampler sampler() {
        return this.sampler;
    }

    @Override
    public boolean enabled() {
        return this.sampler.enabled();
    }

    @Override
    public ResourceSnapshot snapshot() {
        return this.sampler.current();
    }

    @Override
    public Optional<PoolSnapshot> pool(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        return this.sampler.current().pool(pool);
    }

    @Override
    public SaturationReport saturation(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        return this.sampler.current()
            .pool(pool)
            .map(PoolSnapshot::saturation)
            .orElseGet(() -> SaturationReport.unknown(pool));
    }

    @Override
    public List<ThreadSnapshot> threads(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        return this.sampler.current()
            .threads()
            .stream()
            .filter(thread -> thread.pool().equals(pool))
            .toList();
    }

    @Override
    public List<ThreadSnapshot> unregisteredThreads() {
        return this.sampler.unregisteredThreads();
    }

    @Override
    public double creationRatePerSecond() {
        return this.sampler.current().creationRatePerSecond();
    }

    @Override
    public double terminationRatePerSecond() {
        return this.sampler.current().terminationRatePerSecond();
    }

    @Override
    public int totalThreads() {
        return this.sampler.current().totalThreads();
    }

    @Override
    public int activeThreads() {
        return this.sampler.current().activeThreads();
    }

    @Override
    public int idleThreads() {
        return this.sampler.current().idleThreads();
    }
}
