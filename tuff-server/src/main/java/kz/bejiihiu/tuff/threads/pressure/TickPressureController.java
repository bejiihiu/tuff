package kz.bejiihiu.tuff.threads.pressure;

import java.util.Objects;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;

/**
 * Decides how many cores the compute pool gives up while regions lag, and when to take them back.
 *
 * <p>Three guards keep the pool from flapping. <b>Hysteresis:</b> entering needs a slow region below
 * {@code enterMinTps} (or a big enough lagging share), leaving needs no lagging region at all and the
 * slowest region at or above {@code exitMinTps}; the band between the two thresholds moves nothing.
 * <b>Sustained calm:</b> the reserve is released only after {@code exitWindows} consecutive calm
 * samples. <b>Cooldown:</b> no reserve change is applied more often than {@code cooldownSeconds}, so a
 * sawtooth signal cannot resize the pool every second.
 *
 * <p>Changes are ramps, not jumps: every decision moves at most {@code stepCores} cores, and the
 * reserve never exceeds the ceiling the owner passes in (which is where the compute pool's
 * {@code min-threads} floor comes from). A ceiling of zero means pressure cannot help this machine,
 * and the controller says so by staying at zero instead of pretending.
 *
 * <p>Not thread safe: the monitor calls it from the sampler thread only. The published {@link #reserve()}
 * is volatile so the budget reader on another thread sees the last applied number, and the report reads
 * the immutable snapshot the monitor publishes, never this object.
 */
public final class TickPressureController {

    private final ThreadManagementSettings.AsyncCompute.TickPressure settings;
    private final long cooldownNanos;

    private volatile int reserve;
    private volatile State state = State.NORMAL;
    private volatile Reason lastReason = Reason.STARTUP;
    private int calmWindows;
    private long lastChangeNanos = Long.MIN_VALUE;

    /**
     * Creates a controller for one policy.
     *
     * @param settings pressure policy
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    public TickPressureController(final ThreadManagementSettings.AsyncCompute.TickPressure settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.cooldownNanos = settings.cooldownNanos();
    }

    /**
     * Evaluates one sample and moves the reserve if the policy allows it.
     *
     * <p>Called once per sampler pass. A sample that could not measure the regions changes nothing:
     * the controller keeps its current reserve instead of guessing.
     *
     * @param signal         the region health reading
     * @param reserveCeiling most cores the owner allows the reserve to reach, at least zero
     * @param nowNanos       monotonic timestamp of the sample
     * @return what the decision was and whether it moved the reserve
     * @throws NullPointerException if {@code signal} is {@code null}
     */
    public Decision evaluate(final TickSignal signal, final int reserveCeiling, final long nowNanos) {
        Objects.requireNonNull(signal, "signal");
        if (!this.settings.enabled()) {
            return new Decision(false, this.state, this.reserve, Reason.DISABLED);
        }
        if (!signal.measured()) {
            this.lastReason = Reason.SIGNAL_UNAVAILABLE;
            return new Decision(false, this.state, this.reserve, Reason.SIGNAL_UNAVAILABLE);
        }

        final int ceiling = Math.max(0, reserveCeiling);
        if (this.reserve > ceiling) {
            // the owner's ceiling moved under us (reserve cores changed, min-threads grew): honour it
            // now rather than at the next lag window, the pool is oversubscribed until we do
            this.reserve = ceiling;
            this.lastChangeNanos = nowNanos;
            if (this.reserve == 0) {
                this.state = State.NORMAL;
            }
            this.lastReason = Reason.CEILING_REACHED;
            return new Decision(true, this.state, this.reserve, Reason.CEILING_REACHED);
        }

        final boolean lagging = signal.lagging(this.settings.enterLagFraction(), this.settings.enterMinTps());
        final boolean calm = signal.calm(this.settings.exitMinTps());
        final boolean ready = this.lastChangeNanos == Long.MIN_VALUE
            || nowNanos - this.lastChangeNanos >= this.cooldownNanos;

        if (this.state == State.NORMAL) {
            if (lagging) {
                if (ceiling == 0) {
                    this.lastReason = Reason.CEILING_REACHED;
                    return new Decision(false, this.state, this.reserve, Reason.CEILING_REACHED);
                }
                if (!ready) {
                    return this.unchanged();
                }
                this.state = State.PRESSURE;
                this.reserve = Math.min(ceiling, this.settings.stepCores());
                this.calmWindows = 0;
                this.lastChangeNanos = nowNanos;
                this.lastReason = Reason.LAG_ENTER;
                return new Decision(true, this.state, this.reserve, Reason.LAG_ENTER);
            }
            this.calmWindows = 0;
            return this.unchanged();
        }

        // PRESSURE
        if (!calm) {
            this.calmWindows = 0;
            if (!lagging) {
                // the deadband: not calm enough to release, not late enough to take more
                return this.unchanged();
            }
            if (this.reserve >= ceiling) {
                this.lastReason = Reason.CEILING_REACHED;
                return new Decision(false, this.state, this.reserve, Reason.CEILING_REACHED);
            }
            if (!ready) {
                return this.unchanged();
            }
            this.reserve = Math.min(ceiling, this.reserve + this.settings.stepCores());
            this.lastChangeNanos = nowNanos;
            this.lastReason = Reason.LAG_STEP_UP;
            return new Decision(true, this.state, this.reserve, Reason.LAG_STEP_UP);
        }

        this.calmWindows++;
        if (this.calmWindows < this.settings.exitWindows() || !ready) {
            return this.unchanged();
        }
        this.calmWindows = 0;
        this.reserve = Math.max(0, this.reserve - this.settings.stepCores());
        this.lastChangeNanos = nowNanos;
        if (this.reserve == 0) {
            this.state = State.NORMAL;
            this.lastReason = Reason.CALM_EXIT;
            return new Decision(true, this.state, this.reserve, Reason.CALM_EXIT);
        }
        this.lastReason = Reason.CALM_STEP_DOWN;
        return new Decision(true, this.state, this.reserve, Reason.CALM_STEP_DOWN);
    }

    private Decision unchanged() {
        return new Decision(false, this.state, this.reserve, this.lastReason);
    }

    /**
     * Returns how many cores the compute pool has yielded right now.
     *
     * @return the reserve, never negative
     */
    public int reserve() {
        return this.reserve;
    }

    /**
     * Where the pool stands.
     */
    public enum State {

        /**
         * No cores are yielded; the compute pool runs on its whole budget.
         */
        NORMAL,

        /**
         * At least one core is yielded to the regions.
         */
        PRESSURE
    }

    /**
     * Why the controller moved or refused to move the reserve.
     */
    public enum Reason {

        /**
         * No decision has been made yet.
         */
        STARTUP,

        /**
         * {@code tick-pressure.enabled: false}; nothing is decided.
         */
        DISABLED,

        /**
         * The sample could not read the regions; the previous reserve is kept.
         */
        SIGNAL_UNAVAILABLE,

        /**
         * Regions started to lag and the reserve went from zero to one step.
         */
        LAG_ENTER,

        /**
         * The regions are still behind after a cooldown; another step was yielded.
         */
        LAG_STEP_UP,

        /**
         * One calm window after a cooldown; a step was returned to the pool.
         */
        CALM_STEP_DOWN,

        /**
         * The reserve reached zero, the pool has its whole budget again.
         */
        CALM_EXIT,

        /**
         * The owner's ceiling shrank, or there is no headroom to yield; the reserve is clamped.
         */
        CEILING_REACHED
    }

    /**
     * One evaluation's outcome.
     *
     * @param changed whether the reserve moved
     * @param state   state after the decision
     * @param reserve reserve after the decision
     * @param reason  why the decision was made
     */
    public record Decision(boolean changed, State state, int reserve, Reason reason) {
    }
}
