package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ThreadFactoryProvider;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Bridges the compute engine to the unified thread layer, without either module knowing the other.
 *
 * <p>The engine asks two ports: where its threads come from and what its processor budget is. This
 * adapter answers both from the thread layer - the factory provider creates and registers the worker
 * threads, the resource controller owns the budget - and registers the compute pools at
 * {@link CoverageLevel#L4} so the layer knows what it is accounting for.
 *
 * <p>Behaviour is decided by one flag, {@code execution-pools.compute.managed} (or its older spelling
 * {@code async-compute.managed}). With the flag off the adapter never hands a port to the engine: the
 * engine keeps its own factory and its own budget arithmetic, and this class only attaches the probes
 * so the pool shows up in snapshots. With the flag on the engine creates every thread through
 * {@link ThreadFactoryProvider} and reads its budget from {@link ThreadResourceController}.
 *
 * <p>The names are the operational contract: thread dumps, the shutdown report and {@code dumpWorkers}
 * all search by the worker prefix, so the adapter never renames anything. Platform workers come out as
 * {@code <prefix>-<n>} from the provider, exactly like the legacy factory; the one-off service threads
 * (janitor, shutdown, lane pump) keep their exact names because they are built by the legacy factory
 * and only have their bodies wrapped for accounting - the provider always appends a counter, which
 * would break those names.
 *
 * <p>A refused provider never kills the engine. Provider creation can fail when the pool is not
 * registered yet or the manager already shut down; in that case the adapter logs once and returns the
 * legacy factory, so the pool degrades to "unaccounted threads" instead of "engine failed".
 */
public final class ComputeUtmiAdapter implements ComputeThreadFactoryPort, ComputeBudgetPort {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Compute");

    private static final String OWNER = "tuff-compute";

    /**
     * Fallback handler for a thread the engine has not adopted yet; the pool installs its own handler
     * right after creation, so this only covers a thread that dies before adoption.
     */
    private static final Thread.UncaughtExceptionHandler FAILURE =
        (thread, failure) -> LOGGER.error("Compute thread {} died", thread.getName(), failure);

    private final ThreadManagerRuntime runtime;
    private final UnifiedThreadManager manager;
    private final ThreadFactoryProvider provider;
    private final ThreadResourceController controller;
    private final ComputeSettings settings;
    private final ComputeThreadFactoryPort legacy = ComputeThreadFactoryPort.legacy();
    private final ComputePoolProbe platformProbe;
    private final ComputePoolProbe virtualProbe;
    private final AtomicBoolean fallbackLogged = new AtomicBoolean();

    private volatile boolean managed;
    private volatile @Nullable AsyncComputeScheduler engine;

    private ComputeUtmiAdapter(
        final ThreadManagerRuntime runtime,
        final UnifiedThreadManager manager,
        final ComputeSettings settings
    ) {
        this.runtime = runtime;
        this.manager = manager;
        this.settings = settings;
        this.provider = manager.threadFactory();
        this.controller = manager.resources();
        final String prefix = settings.workers().threadNamePrefix();
        this.platformProbe = new ComputePoolProbe(
            PoolId.COMPUTE, this::engineMetrics, prefix, settings.workers().minThreads()
        );
        this.virtualProbe = new ComputePoolProbe(PoolId.COMPUTE_VIRTUAL, this::engineMetrics, prefix, 0);
    }

    /**
     * Binds the adapter to a running thread manager and registers the compute pools.
     *
     * <p>Must be called before the engine starts: the provider refuses to create a worker for a pool
     * that is not registered, and the engine spawns its minimum workers inside {@code start()}.
     *
     * @param runtime  running thread manager runtime
     * @param settings compute configuration the engine will run with
     * @return the adapter, never {@code null}
     * @throws NullPointerException  if an argument is {@code null}
     * @throws IllegalStateException if the thread manager is not running
     */
    public static ComputeUtmiAdapter attach(
        final ThreadManagerRuntime runtime,
        final ComputeSettings settings
    ) {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(settings, "settings");
        final UnifiedThreadManager manager = runtime.manager();
        if (!manager.isRunning()) {
            throw new IllegalStateException("the thread manager is " + manager.state() + ", not running");
        }
        final ComputeUtmiAdapter adapter = new ComputeUtmiAdapter(runtime, manager, settings);
        adapter.register();
        return adapter;
    }

    /**
     * Tells the adapter which engine to observe.
     *
     * <p>Called right after the engine started, because the probes read the engine's own counters:
     * worker counts, the busy flag of every worker and the queue depth. Before this call the probes
     * report an empty pool.
     *
     * @param scheduler running engine, or {@code null} once it stopped
     */
    public void bind(final @Nullable AsyncComputeScheduler scheduler) {
        this.engine = scheduler;
    }

    /**
     * Checks whether the engine should take its threads and its budget from this adapter.
     *
     * @return {@code true} when the pool is registered at L4 and the provider accepted it
     */
    public boolean isManaged() {
        return this.managed;
    }

    /**
     * Returns the platform pool probe, for diagnostics and tests.
     *
     * @return the probe bound to {@link PoolId#COMPUTE}
     */
    PoolProbe platformProbe() {
        return this.platformProbe;
    }

    /**
     * Returns the virtual pool probe, for diagnostics and tests.
     *
     * @return the probe bound to {@link PoolId#COMPUTE_VIRTUAL}
     */
    PoolProbe virtualProbe() {
        return this.virtualProbe;
    }

    @Override
    public int processors() {
        // the controller's budget is already net of the reserves; asking it here is the whole point of
        // the port, so nothing below subtracts region, chunk or IO threads a second time
        final int usable = Math.max(1, this.controller.usableCores());
        return this.controller.budget(PoolId.COMPUTE)
            .map(budget -> Math.max(1, budget.resolveCores(usable)))
            .orElse(usable);
    }

    @Override
    public String source() {
        return "utmi";
    }

    /**
     * Returns the controller's budget ledger, one printable line.
     *
     * <p>Startup logs it next to the applied budget so "why does the pool have that many workers" can
     * be answered from the console.
     *
     * @return the ledger line
     */
    public String budgetLedger() {
        return this.controller.budgetLedger();
    }

    @Override
    public ThreadFactory platform(final String namePrefix, final boolean daemon) {
        if (!this.managed) {
            return this.legacy.platform(namePrefix, daemon);
        }
        final ThreadSpec spec = ThreadSpec.of(PoolId.COMPUTE, namePrefix, FAILURE).withDaemon(daemon);
        final @Nullable ThreadFactory managedFactory = this.providerFactory(spec, namePrefix);
        if (managedFactory == null) {
            return this.legacy.platform(namePrefix, daemon);
        }
        // the fallback only fires when the provider refuses a late respawn (shutdown window); its
        // counter restarts at one, which is fine for a fleet that is already dying
        final ThreadFactory fallback = this.legacy.platform(namePrefix, daemon);
        return body -> {
            try {
                return managedFactory.newThread(body);
            } catch (IllegalStateException ex) {
                this.warnFallback(namePrefix, ex);
                return fallback.newThread(body);
            }
        };
    }

    @Override
    public ThreadFactory virtual(final String namePrefix) {
        final ThreadFactory base = this.legacy.virtual(namePrefix);
        if (!this.managed || !this.manages(PoolId.COMPUTE_VIRTUAL)) {
            return base;
        }
        // the provider refuses to create virtual threads on purpose - the lane owns them - so the
        // thread is built here and only its body is wrapped for accounting
        return body -> base.newThread(this.accounted(PoolId.COMPUTE_VIRTUAL, namePrefix, body));
    }

    @Override
    public ThreadFactory named(final String name, final boolean daemon) {
        final ThreadFactory base = this.legacy.named(name, daemon);
        if (!this.managed) {
            return base;
        }
        return body -> base.newThread(this.accounted(PoolId.COMPUTE, name, body));
    }

    private void register() {
        if (!this.manager.settings().computeManaged()) {
            this.attachProbes();
            LOGGER.info("Tuff compute: the thread layer observes the engine only "
                + "(execution-pools.compute.managed: false); the engine keeps its own threads and budget");
            return;
        }
        final int minThreads = this.settings.workers().minThreads();
        final int maxThreads = this.settings.workers().maxThreads();
        try {
            this.manager.pools().register(new PoolSpec(
                PoolId.COMPUTE,
                PoolKind.COMPUTE,
                OWNER,
                "platform workers of the async compute engine",
                minThreads,
                maxThreads,
                CoverageLevel.L4,
                ResourceBudget.elastic(1.0d, 0)
            ));
            final ComputeSettings.Lanes.Virtual lane = this.settings.lanes().virtual();
            this.manager.pools().register(lane.enabled()
                ? new PoolSpec(
                    PoolId.COMPUTE_VIRTUAL,
                    PoolKind.VIRTUAL,
                    OWNER,
                    "virtual lane of the async compute engine",
                    1,
                    lane.maxConcurrent(),
                    // L2, not L4: the manager accounts the lane's threads but does not own its queue or
                    // its permits. The declared budget is the one platform thread the lane really has
                    // (the pump), so the lane cannot claim a share of the compute budget in the ledger
                    CoverageLevel.L2,
                    ResourceBudget.fixed(1, 0)
                )
                : PoolSpec.observeOnly(
                    PoolId.COMPUTE_VIRTUAL,
                    PoolKind.VIRTUAL,
                    OWNER,
                    "virtual lane of the async compute engine"
                ));
            this.managed = true;
            this.attachProbes();
            LOGGER.info("Tuff compute: the thread layer manages the pool (min {} worker(s), max {}, budget source {}); "
                + "budget ledger: {}",
                minThreads, maxThreads == -1 ? "auto" : Integer.toString(maxThreads), this.source(), this.budgetLedger());
        } catch (RuntimeException ex) {
            // never take the engine down over bookkeeping: it simply keeps its legacy threads
            this.managed = false;
            this.attachProbes();
            LOGGER.error("Tuff compute: the thread layer could not take the pool over; the engine keeps its own "
                + "threads and its own budget", ex);
        }
    }

    private void attachProbes() {
        try {
            this.runtime.attachProbe(this.platformProbe);
            this.runtime.attachProbe(this.virtualProbe);
        } catch (RuntimeException ex) {
            LOGGER.warn("Tuff compute: the thread layer accepted the pools but not their probes", ex);
        }
    }

    private @Nullable ThreadFactory providerFactory(final ThreadSpec spec, final String label) {
        try {
            return this.provider.factory(spec);
        } catch (IllegalStateException ex) {
            this.warnFallback(label, ex);
            return null;
        }
    }

    private Runnable accounted(final PoolId pool, final String name, final Runnable body) {
        try {
            return this.provider.wrap(ThreadSpec.of(pool, name, FAILURE), body);
        } catch (IllegalStateException ex) {
            this.warnFallback(name, ex);
            return body;
        }
    }

    private boolean manages(final PoolId pool) {
        return this.manager.pools().spec(pool)
            .map(spec -> spec.coverage().managesThreads())
            .orElse(false);
    }

    private @Nullable SchedulerMetrics engineMetrics() {
        final AsyncComputeScheduler current = this.engine;
        return current == null ? null : current.metrics();
    }

    private void warnFallback(final String label, final IllegalStateException failure) {
        if (this.fallbackLogged.compareAndSet(false, true)) {
            LOGGER.warn("Tuff compute: the thread layer refused '{}'; the engine falls back to its own factory for the "
                + "rest of this run", label, failure);
        } else {
            LOGGER.debug("Compute thread '{}' fell back to the legacy factory: {}", label, failure.getMessage());
        }
    }
}
