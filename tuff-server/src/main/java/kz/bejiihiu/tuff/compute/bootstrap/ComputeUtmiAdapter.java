package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
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
import kz.bejiihiu.tuff.threads.pressure.PressureBudgetSink;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
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
    private final RegionReliefSettings relief;
    private final ComputeThreadFactoryPort legacy = ComputeThreadFactoryPort.legacy();
    private final ComputePoolProbe platformProbe;
    private final ComputePoolProbe virtualProbe;
    private final @Nullable TickPressureMonitor pressure;
    private final PressureBudgetSink pressureSink = new AdapterPressureSink();
    private final AtomicBoolean fallbackLogged = new AtomicBoolean();

    private volatile boolean managed;
    private volatile @Nullable AsyncComputeScheduler engine;

    private ComputeUtmiAdapter(
        final ThreadManagerRuntime runtime,
        final UnifiedThreadManager manager,
        final ComputeSettings settings,
        final RegionReliefSettings relief,
        final @Nullable TickPressureMonitor pressure
    ) {
        this.runtime = runtime;
        this.manager = manager;
        this.settings = settings;
        this.relief = relief;
        this.pressure = pressure;
        this.provider = manager.threadFactory();
        this.controller = manager.resources();
        // load shedding moves a pool's effective capacity without a lease; the engine has to hear about
        // it on the same admission decision, not on the next pressure sample
        this.controller.addCapacityListener(this::capacityChanged);
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
        return attach(runtime, settings, null, RegionReliefSettings.defaults());
    }

    /**
     * Binds the adapter to a running thread manager and registers the compute pools, with the tick
     * pressure monitor that may lower the pool's budget.
     *
     * @param runtime  running thread manager runtime
     * @param settings compute configuration the engine will run with
     * @param pressure tick pressure monitor, or {@code null} when the policy is off
     * @return the adapter, never {@code null}
     * @throws NullPointerException  if a required argument is {@code null}
     * @throws IllegalStateException if the thread manager is not running
     */
    public static ComputeUtmiAdapter attach(
        final ThreadManagerRuntime runtime,
        final ComputeSettings settings,
        final @Nullable TickPressureMonitor pressure
    ) {
        return attach(runtime, settings, pressure, RegionReliefSettings.defaults());
    }

    /**
     * Binds the adapter with the region relief settings that own the protected core reserve.
     *
     * @param runtime  running thread manager runtime
     * @param settings compute configuration the engine will run with
     * @param pressure tick pressure monitor, or {@code null} when the policy is off
     * @param relief   region relief configuration
     * @return the adapter, never {@code null}
     * @throws NullPointerException  if a required argument is {@code null}
     * @throws IllegalStateException if the thread manager is not running
     */
    public static ComputeUtmiAdapter attach(
        final ThreadManagerRuntime runtime,
        final ComputeSettings settings,
        final @Nullable TickPressureMonitor pressure,
        final RegionReliefSettings relief
    ) {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(relief, "relief");
        final UnifiedThreadManager manager = runtime.manager();
        if (!manager.isRunning()) {
            throw new IllegalStateException("the thread manager is " + manager.state() + ", not running");
        }
        final ComputeUtmiAdapter adapter = new ComputeUtmiAdapter(runtime, manager, settings, relief, pressure);
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
        // the controller's budget is already net of the reserves and of any shed, so the pool budget is
        // the one base both the startup number and every later resize use. the tick pressure reserve is
        // the one subtraction on top: regions are late and the pool gives cores back. the min-threads
        // floor wins over both, because the engine will not run fewer workers than that anyway
        final int poolBudget = this.poolBudget(Math.max(1, this.controller.usableCores()));
        final @Nullable TickPressureMonitor monitor = this.pressure;
        return this.applyReserve(poolBudget, monitor == null ? 0 : monitor.reserve());
    }

    /**
     * Returns the compute pool's effective capacity, after load shedding and before pressure.
     *
     * <p>This is the number the adapter answers to {@link PressureBudgetSink#poolBudget(int)}; the
     * monitor uses the same value so the printed budget and the applied one cannot diverge.
     *
     * @param usableCores budget ledger's usable cores, at least one
     * @return the pool budget, at least one
     */
    private int poolBudget(final int usableCores) {
        return this.controller.budget(PoolId.COMPUTE)
            .map(budget -> Math.max(1, budget.resolveCores(usableCores)))
            .orElse(usableCores);
    }

    /**
     * Applies the tick pressure reserve to a pool budget, never below the budget the engine needs for
     * {@code min-threads} plus the relief reserve.
     *
     * @param poolBudget cores the pool owns right now
     * @param reserve    cores it is yielding to the regions
     * @return the processor budget to hand to the engine, at least the configured floor
     */
    private int applyReserve(final int poolBudget, final int reserve) {
        // the floor is absolute, exactly like min-threads itself: a budget smaller than the floor still
        // runs the floor, because the engine refuses to go below it anyway
        return Math.max(this.reliefBudgetFloor(), TickPressureMonitor.budgetFor(poolBudget, reserve));
    }

    /**
     * Returns the budget the engine needs to run {@code min-threads + reserved-cores} workers.
     *
     * <p>Automatic sizing keeps one processor as headroom, so a budget of {@code N} resolves to
     * {@code N - 1} workers. Reserving cores at the budget level alone would therefore reserve nothing
     * at the worker level whenever {@code min-threads} is already binding; the extra budget unit is
     * what makes the reserve show up as an actual worker. A fixed pool ignores the budget entirely, so
     * for it the reserve is only an informational number.
     *
     * @return the lowest processor budget pressure may apply, never below {@code min-threads}
     */
    private int reliefBudgetFloor() {
        final int floor = Math.max(1, this.settings.workers().minThreads());
        final int reserved = this.relief.effectiveReservedCores();
        if (reserved == 0) {
            return floor;
        }
        return this.settings.workers().automaticThreadCount() ? floor + reserved + 1 : floor + reserved;
    }

    /**
     * Returns the cores the pool must keep for relief no matter what pressure decides.
     *
     * @return the effective relief reserve, never negative
     */
    int reliefReserveCores() {
        return this.relief.effectiveReservedCores();
    }

    /**
     * Returns the sink that applies a tick pressure reserve to the engine.
     *
     * <p>The thread layer binds it only when the pool is managed; an observed pool keeps logging its
     * decisions without touching the engine's size.
     *
     * @return the sink, never {@code null}
     */
    public PressureBudgetSink pressureSink() {
        return this.pressureSink;
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
        // the lane's pump is a platform thread of the virtual pool, not a compute worker: classifying
        // it as compute made the pump show up twice (registry plus the lane's own count) and under the
        // wrong pool
        final PoolId pool = this.virtualPump(name) && this.manages(PoolId.COMPUTE_VIRTUAL)
            ? PoolId.COMPUTE_VIRTUAL
            : PoolId.COMPUTE;
        return body -> base.newThread(this.accounted(pool, name, body));
    }

    private boolean virtualPump(final String name) {
        return name.equals(this.settings.workers().threadNamePrefix() + "-virtual-pump");
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
                : new PoolSpec(
                    PoolId.COMPUTE_VIRTUAL,
                    PoolKind.VIRTUAL,
                    OWNER,
                    "virtual lane of the async compute engine (disabled)",
                    0,
                    -1,
                    // a lane that is off owns nothing: registering it with the observe-only elastic
                    // default made it a claimant of the shared remainder and halved the compute budget
                    CoverageLevel.L1,
                    ResourceBudget.fixed(0, 0)
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

    /**
     * Re-applies the budget after the controller moved the pool's capacity.
     *
     * <p>Load shedding is the case: it withholds a core from the compute pool on an admission or release
     * decision, and waiting for the next pressure sample would leave the engine sized for capacity it no
     * longer has. The callback is registered once and calls the same {@code applyProcessorBudget} the
     * pressure sink uses, so there is still one writer.
     */
    private void capacityChanged() {
        if (!this.managed) {
            return;
        }
        final @Nullable AsyncComputeScheduler current = this.engine;
        if (current == null) {
            return;
        }
        try {
            current.applyProcessorBudget(this.processors());
        } catch (RuntimeException ex) {
            LOGGER.warn("Tuff compute: the engine refused the budget after a capacity change", ex);
        }
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

    /**
     * Applies a reserve the tick pressure controller decided on.
     *
     * <p>The reserve is capped by {@code workers.min-threads} on this machine: the pool never gives
     * away so many cores that fewer than its minimum workers fit in the budget. Everything goes
     * through {@link AsyncComputeScheduler#applyProcessorBudget(int)}, the same single entry point the
     * startup budget uses, so there is no second way to size the pool.
     */
    private final class AdapterPressureSink implements PressureBudgetSink {

        @Override
        public int poolBudget(final int usableCores) {
            return ComputeUtmiAdapter.this.poolBudget(Math.max(1, usableCores));
        }

        @Override
        public int minBudget() {
            // the same floor the budget port applies, so the printed decision and the engine cannot diverge
            return ComputeUtmiAdapter.this.reliefBudgetFloor();
        }

        @Override
        public int maxReserve(final int usableCores) {
            final @Nullable TickPressureMonitor monitor = ComputeUtmiAdapter.this.pressure;
            if (monitor == null) {
                return 0;
            }
            final int configured = Math.max(0, monitor.maxReserveCores());
            // the floor applies to the pool budget - the same number the engine is sized from - not to
            // the ledger's usable. it already carries min-threads and the relief reserve, including
            // the headroom unit that turns the reserve into an actual worker
            final int floor = Math.max(0,
                ComputeUtmiAdapter.this.poolBudget(Math.max(1, usableCores))
                    - ComputeUtmiAdapter.this.reliefBudgetFloor());
            return Math.min(configured, floor);
        }

        @Override
        public void reserveChanged(final int reserve) {
            final @Nullable AsyncComputeScheduler current = ComputeUtmiAdapter.this.engine;
            if (!ComputeUtmiAdapter.this.managed) {
                LOGGER.debug("Tuff compute pressure: reserve {} core(s) not applied, the pool is observed only", reserve);
                return;
            }
            if (current == null) {
                // the engine has not started yet; its startup budget already asks processors() for the
                // current reserve, so nothing is lost
                LOGGER.debug("Tuff compute pressure: reserve {} core(s) not applied yet, the engine is not bound",
                    reserve);
                return;
            }
            final int budget = ComputeUtmiAdapter.this.processors();
            current.applyProcessorBudget(budget);
            LOGGER.debug("Tuff compute pressure: pool budget is now {} ({} core(s) yielded, {} usable)",
                budget, reserve, ComputeUtmiAdapter.this.controller.usableCores());
        }
    }
}
