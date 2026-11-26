package kz.bejiihiu.tuff.threads.internal.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.impl.controller.ResourceControllerImpl;
import kz.bejiihiu.tuff.threads.internal.impl.events.MetricsEventSink;
import kz.bejiihiu.tuff.threads.internal.impl.factory.ThreadFactoryProviderImpl;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.MetricsConfig;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.MxBeanThreadCounters;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceMetricsImpl;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceSampler;
import kz.bejiihiu.tuff.threads.internal.impl.pools.ExecutionPoolRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.impl.pools.PoolProbeRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.impl.registry.ThreadRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;
import org.jspecify.annotations.Nullable;

/**
 * Owns the assembled components and drives their lifecycle.
 *
 * <p>Assembly order is dependency order, not taste: the registries exist before anything can look a
 * pool up, the topology exists before the controller sizes a ledger, and the controller exists before
 * the sampler because the sampler reads lease waits from it. Getting this order wrong is silent - a
 * sampler built without the controller reports zero lease wait forever.
 *
 * <p>Shutdown order is the reverse of authority: pools stop accepting specs, the factory stops creating
 * threads, the registry stops accounting new ones, probes stop attaching, then the sampler closes and
 * only then admission closes. The sampler goes before the controller so nothing measures inside the
 * window where the ledger is being closed. No step joins, sleeps or waits: this path runs where a slow
 * call is indistinguishable from a hung server.
 */
final class ThreadManagerRuntimeImpl implements ThreadManagerRuntime {

    private final ThreadManagementSettings settings;
    private final ExecutionPoolRegistryImpl pools;
    private final ThreadRegistryImpl registry;
    private final ThreadFactoryProviderImpl factory;
    private final PoolProbeRegistryImpl probes;
    private final ResourceControllerImpl controller;
    private final ResourceSampler sampler;
    private final ThreadManagerImpl manager;
    private final @Nullable Consumer<String> stopSteps;

    private final Object lifecycle = new Object();

    /**
     * Builds every component without registering a pool or creating a thread.
     *
     * @param settings  effective configuration
     * @param clock     monotonic clock for leases and sampling windows
     * @param topology  processor topology the controller sizes against
     * @param sink      event sink for lifecycle and pressure events
     * @param stopSteps optional observer called once per shutdown step, for tests and diagnostics
     */
    ThreadManagerRuntimeImpl(
        final ThreadManagementSettings settings,
        final MonotonicClock clock,
        final TopologyView topology,
        final MetricsEventSink sink,
        final @Nullable Consumer<String> stopSteps
    ) {
        this.settings = Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(sink, "sink");
        this.stopSteps = stopSteps;

        this.pools = new ExecutionPoolRegistryImpl();
        this.registry = new ThreadRegistryImpl(sink);
        this.factory = new ThreadFactoryProviderImpl(this.pools, this.registry);
        this.controller = new ResourceControllerImpl(
            topology,
            this.pools,
            this.registry,
            settings.resources(),
            DefaultPoolSpecs.deductions(settings),
            clock,
            sink
        );
        this.probes = new PoolProbeRegistryImpl(this.pools);
        this.sampler = new ResourceSampler(
            this.pools,
            this.registry,
            this.probes::attached,
            new MxBeanThreadCounters(),
            clock,
            sink,
            metricsConfig(settings),
            settings.resources().reserveCores(),
            this.controller
        );
        this.manager = new ThreadManagerImpl(
            settings,
            this.pools,
            this.controller,
            this.factory,
            new ResourceMetricsImpl(this.sampler),
            this.sampler
        );
    }

    @Override
    public UnifiedThreadManager manager() {
        return this.manager;
    }

    @Override
    public void start() {
        synchronized (this.lifecycle) {
            switch (this.manager.phase()) {
                case STARTED -> {
                    return;
                }
                case SHUTTING_DOWN, STOPPED -> throw new IllegalStateException(
                    "the thread manager already stopped and cannot be started again"
                );
                case NEW -> {
                    // registration happens here, never in the constructor: a runtime that is created
                    // but not started must leave the registry exactly as it found it
                }
            }
            for (final PoolSpec spec : DefaultPoolSpecs.collect(this.settings)) {
                this.pools.register(spec);
            }
            // the ledger was built for an empty registry; make it describe what was just registered,
            // otherwise "usable" and "budget" would answer for a world that no longer exists
            this.controller.refresh();
            this.manager.markStarted();
            this.lifecycle.notifyAll();
        }
    }

    @Override
    public void shutdown(final Duration gracePeriod) {
        Objects.requireNonNull(gracePeriod, "gracePeriod");
        if (gracePeriod.isNegative()) {
            throw new IllegalArgumentException("gracePeriod must not be negative, got " + gracePeriod);
        }
        synchronized (this.lifecycle) {
            final ThreadManagerImpl.Phase phase = this.manager.phase();
            if (phase == ThreadManagerImpl.Phase.STOPPED || phase == ThreadManagerImpl.Phase.SHUTTING_DOWN) {
                return;
            }
            // the grace period is advisory here: the manager owns no executor and no thread of its own,
            // so there is nothing to wait for inside the server's shutdown window. Callers that need to
            // bound the accounting tail use awaitTermination, which is allowed to block.
            this.manager.markShuttingDown();
            this.stopStep("pools");
            this.pools.markStopped();
            this.stopStep("factory");
            this.factory.markStopped();
            this.stopStep("registry");
            this.registry.markStopped();
            this.stopStep("probes");
            this.probes.markStopped();
            this.stopStep("sampler");
            this.sampler.close();
            this.stopStep("controller");
            this.controller.close();
            this.manager.markStopped();
            this.lifecycle.notifyAll();
        }
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative, got " + timeout);
        }
        final long waitNanos = nanos(timeout);
        synchronized (this.lifecycle) {
            final long deadline = saturatingAdd(System.nanoTime(), waitNanos);
            long remaining = waitNanos;
            while (this.manager.phase() != ThreadManagerImpl.Phase.STOPPED) {
                if (remaining <= 0L) {
                    return false;
                }
                this.lifecycle.wait(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
                remaining = deadline - System.nanoTime();
            }
            return true;
        }
    }

    /**
     * Reports a shutdown step to the optional observer.
     *
     * @param step stable step name
     */
    private void stopStep(final String step) {
        if (this.stopSteps != null) {
            this.stopSteps.accept(step);
        }
    }

    /**
     * Builds the sampling policy, honouring both switches that can turn sampling off.
     *
     * @param settings effective configuration
     * @return the policy
     */
    private static MetricsConfig metricsConfig(final ThreadManagementSettings settings) {
        final MetricsConfig base = MetricsConfig.from(settings.metrics());
        final boolean enabled = base.enabled() && settings.lifecycle().monitorThreads();
        return new MetricsConfig(
            enabled,
            base.perPool(),
            base.perThread(),
            base.interval(),
            base.unregisteredInterval()
        );
    }

    private static long nanos(final Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException _) {
            return Long.MAX_VALUE;
        }
    }

    private static long saturatingAdd(final long base, final long delta) {
        final long sum = base + delta;
        if (((base ^ sum) & (delta ^ sum)) < 0L) {
            return Long.MAX_VALUE;
        }
        return sum;
    }
}
