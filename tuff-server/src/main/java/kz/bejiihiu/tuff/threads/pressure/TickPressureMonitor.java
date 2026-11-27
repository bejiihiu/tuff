package kz.bejiihiu.tuff.threads.pressure;

import java.util.Locale;
import java.util.Objects;
import java.util.function.IntSupplier;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Turns the sampler's passes into compute budget changes.
 *
 * <p>The thread layer already pays for one pass per second over regions, probes and threads. This
 * monitor is a listener on that pass: it reads the region summary out of the snapshot, asks the
 * controller for a decision and, when the reserve moved, calls the one sink that can resize the pool.
 * It owns no thread, no queue and no timer, so a stopped server cannot leak one, and nothing here ever
 * runs on a region tick thread.
 *
 * <p>When no sink is bound (the compute pool is not managed, or the engine is not up yet) the decisions
 * and their reasons still go to the log and the snapshot, but nothing is applied. That is the honest
 * behaviour for "the layer observes; the engine owns its threads".
 *
 * <p>The budget formula lives here once: {@code budget = usable - reserve}, floored at one. The adapter
 * calls {@link #budgetFor(int, int)} from its own budget port, so the startup number and every later
 * change agree.
 */
public final class TickPressureMonitor implements TickSampleListener {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Threads");

    private final ThreadManagementSettings.AsyncCompute.TickPressure settings;
    private final IntSupplier usableCores;
    private final TickPressureController controller;

    private volatile @Nullable PressureBudgetSink sink;
    private volatile @Nullable TickPressureSnapshot published;
    private volatile boolean running = true;

    /**
     * Creates a monitor for one policy.
     *
     * @param settings    pressure policy
     * @param usableCores budget ledger's usable cores
     * @throws NullPointerException if an argument is {@code null}
     */
    public TickPressureMonitor(
        final ThreadManagementSettings.AsyncCompute.TickPressure settings,
        final IntSupplier usableCores
    ) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.usableCores = Objects.requireNonNull(usableCores, "usableCores");
        this.controller = new TickPressureController(settings);
    }

    /**
     * Binds the pool that can apply the reserve, replacing the previous one.
     *
     * <p>Called when the compute adapter is created. Until then decisions are logged and published
     * only, which is also the permanent state when the pool is not managed.
     *
     * @param sink the budget sink
     * @throws NullPointerException if {@code sink} is {@code null}
     */
    public void bind(final PressureBudgetSink sink) {
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * Handles one published sampler pass.
     *
     * <p>Never throws: the sampler must keep its one-second clock even when a probe, the ledger or the
     * engine misbehaves. A pass whose snapshot has no region summary makes no decision.
     *
     * @param snapshot     the pass that was just published
     * @param takenAtNanos monotonic timestamp of the pass
     */
    @Override
    public void onSample(final ResourceSnapshot snapshot, final long takenAtNanos) {
        if (!this.running) {
            return;
        }
        try {
            this.evaluate(TickSignal.read(snapshot), takenAtNanos);
        } catch (RuntimeException ex) {
            LOGGER.warn("Tuff compute pressure: could not evaluate the tick signal", ex);
        }
    }

    /**
     * Evaluates one signal and applies the reserve if it moved.
     *
     * <p>Package visible so the tests can drive the state machine without building a server snapshot.
     *
     * @param signal   region health reading
     * @param nowNanos monotonic timestamp
     */
    void evaluate(final TickSignal signal, final long nowNanos) {
        if (!this.running || !this.settings.enabled()) {
            return;
        }
        final int usable = Math.max(1, this.usableCores.getAsInt());
        final @Nullable PressureBudgetSink current = this.sink;
        final int ceiling = current == null
            ? Math.max(0, usable - 1)
            : Math.max(0, current.maxReserve(usable));
        final TickPressureController.Decision decision = this.controller.evaluate(signal, ceiling, nowNanos);
        final int budget = budgetFor(usable, decision.reserve());
        final TickPressureSnapshot snapshot = new TickPressureSnapshot(
            true,
            decision.state(),
            decision.reserve(),
            ceiling,
            budget,
            usable,
            decision.reason(),
            signal
        );
        this.published = snapshot;
        if (!decision.changed()) {
            return;
        }
        LOGGER.info("{}; reserve {} of {} core(s), budget {} of {} ({} region(s), lag {}%, min TPS {}, max MSPT {})",
            describe(decision.reason()),
            decision.reserve(),
            ceiling,
            budget,
            usable,
            signal.regions(),
            Math.round(signal.lagFraction() * 100.0d),
            describeTps(signal.minimumTps()),
            describeMspt(signal.maximumMspt()));
        TickPressureEvent.emit(snapshot);
        if (current == null) {
            LOGGER.info("Tuff compute pressure: no managed compute pool is bound, so nothing was applied "
                + "(enable execution-pools.compute.managed to let the pool yield)");
            return;
        }
        try {
            current.reserveChanged(decision.reserve());
        } catch (RuntimeException ex) {
            LOGGER.warn("Tuff compute pressure: the pool refused the reserve of {} core(s)", decision.reserve(), ex);
        }
    }

    /**
     * Resolves the budget a pool should run at after the reserve.
     *
     * @param usableCores cores the budget ledger reports, at least one
     * @param reserve     cores the pool is yielding, at least zero
     * @return the processor budget, never below one
     */
    public static int budgetFor(final int usableCores, final int reserve) {
        return Math.max(1, usableCores - Math.max(0, reserve));
    }

    /**
     * Returns the reserve currently applied.
     *
     * @return cores the compute pool is yielding
     */
    public int reserve() {
        return this.controller.reserve();
    }

    /**
     * Returns the policy's own reserve ceiling.
     *
     * <p>The adapter lowers it further when the pool's {@code min-threads} does not fit in the
     * budget; this is only the configured number.
     *
     * @return the configured maximum reserve, at least one
     */
    public int maxReserveCores() {
        return this.settings.maxReserveCores();
    }

    /**
     * Returns the last published decision.
     *
     * @return the snapshot, or {@code null} before the first pass with a measurable signal
     */
    public @Nullable TickPressureSnapshot snapshot() {
        return this.published;
    }

    /**
     * Checks whether the monitor still reacts to passes.
     *
     * @return {@code true} until {@link #stop()}
     */
    public boolean isRunning() {
        return this.running;
    }

    /**
     * Stops reacting to passes. Idempotent, never blocks.
     */
    public void stop() {
        this.running = false;
    }

    private static String describe(final TickPressureController.Reason reason) {
        return switch (reason) {
            case LAG_ENTER -> "Tuff compute pressure: regions started to lag, yielding a core";
            case LAG_STEP_UP -> "Tuff compute pressure: regions are still behind, yielding another core";
            case CALM_STEP_DOWN -> "Tuff compute pressure: regions are calm, returning a core";
            case CALM_EXIT -> "Tuff compute pressure: regions are calm, the pool has its whole budget back";
            case CEILING_REACHED -> "Tuff compute pressure: no headroom left to yield";
            case DISABLED -> "Tuff compute pressure: disabled";
            case SIGNAL_UNAVAILABLE -> "Tuff compute pressure: the regions could not be read";
            case STARTUP -> "Tuff compute pressure: first decision";
        };
    }

    private static String describeTps(final double tps) {
        return tps < 0.0d ? "unknown" : String.format(Locale.ROOT, "%.1f", tps);
    }

    private static String describeMspt(final double millis) {
        return millis < 0.0d ? "unknown" : String.format(Locale.ROOT, "%.1f ms", millis);
    }
}
