package kz.bejiihiu.tuff.threads.relief;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.threads.observe.TickObserver;
import kz.bejiihiu.tuff.threads.observe.TickOrigin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Watches the tick feed of every region and opens a relief window when one region keeps overrunning.
 *
 * <p>The feed is the scheduler independent seam: whatever scheduler ran the body, the trigger sees its
 * real start, end and deadline. It never measures a tick itself, so it cannot disagree with the
 * scheduler about when a body ran.
 *
 * <p>A body is an overrun when one of three things holds: it ran for at least the configured overrun
 * time, it started at least a whole period behind its deadline ({@code periodsAhead > 1}), or it
 * started at least the overrun time after its deadline. The last two are the deadline miss, the first
 * is the real body duration.
 *
 * <p>The policy is deliberately asymmetric. A window opens after {@code enterOverruns} consecutive
 * overrun bodies, closes after {@code exitCalm} consecutive calm bodies, stays closed for the cooldown
 * after any close, and one region may open at most {@code maxWindows} windows inside one rate window.
 * Every region has its own counters, so a busy region cannot open or close a window for another one.
 *
 * <p>Regions disappear without a tick. Nothing in the seam reports a region removal, so the trigger
 * bounds the leak instead: a region whose last body is older than {@code staleAfter} is forgotten on
 * the next sweep, and an open window of that region is counted as abandoned. No unit callback runs for
 * an abandoned window, because there is no region thread left to run it on.
 *
 * <p>All state is per region and guarded by that region's own monitor. The trigger is safe to call
 * from any number of region threads at once.
 */
public final class ReliefTrigger implements TickObserver {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Compute");

    /**
     * How many regions the offender list of a snapshot keeps.
     */
    private static final int TOP_OFFENDERS = 5;

    /**
     * Upper bound of the silence sweep interval, so a large {@code staleAfter} does not make the map
     * scan rarer than once per second.
     */
    private static final long SWEEP_CAP_NANOS = 1_000_000_000L;

    private final boolean enabled;
    private final int enterOverruns;
    private final int exitCalm;
    private final int maxWindows;
    private final long overrunNanos;
    private final long cooldownNanos;
    private final long rateWindowNanos;
    private final long staleAfterNanos;
    private final long sweepIntervalNanos;
    private final LongSupplier nanoClock;

    private final Map<Long, RegionState> regions = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<ReliefUnit> units = new CopyOnWriteArrayList<>();
    private final LongAdder windowsEntered = new LongAdder();
    private final LongAdder windowsExited = new LongAdder();
    private final LongAdder windowsAbandoned = new LongAdder();
    private final LongAdder overrunBodies = new LongAdder();
    private final LongAdder calmBodies = new LongAdder();
    private final LongAdder suppressedByCooldown = new LongAdder();
    private final LongAdder suppressedByRate = new LongAdder();
    private final LongAdder unitFailures = new LongAdder();
    private final AtomicLong lastWindowOpenedNanos = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong lastSweepNanos = new AtomicLong();
    private volatile boolean closed;

    /**
     * Creates a trigger for the configured policy.
     *
     * <p>The trigger only acts when both its own switch and the relief switch are on: with relief off
     * a unit's submission would run inline, which would move work into the end of a tick instead of
     * out of it. The combination is decided once, because both keys are restart only.
     *
     * @param settings      trigger policy
     * @param reliefEnabled whether the relief path may offload work at all
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    public ReliefTrigger(final RegionReliefSettings.Trigger settings, final boolean reliefEnabled) {
        this(settings, reliefEnabled, System::nanoTime);
    }

    /**
     * Creates a trigger with an explicit clock, used by tests to keep time deterministic.
     *
     * @param settings      trigger policy
     * @param reliefEnabled whether the relief path may offload work at all
     * @param nanoClock     source of the current time for snapshot ages
     * @throws NullPointerException if {@code settings} or {@code nanoClock} is {@code null}
     */
    ReliefTrigger(
        final RegionReliefSettings.Trigger settings,
        final boolean reliefEnabled,
        final LongSupplier nanoClock
    ) {
        Objects.requireNonNull(settings, "settings");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.enabled = settings.enabled() && reliefEnabled;
        this.enterOverruns = settings.enterOverruns();
        this.exitCalm = settings.exitCalm();
        this.maxWindows = settings.maxWindows();
        this.overrunNanos = settings.overrun().toNanos();
        this.cooldownNanos = settings.cooldown().toNanos();
        this.rateWindowNanos = settings.rateWindow().toNanos();
        this.staleAfterNanos = settings.staleAfter().toNanos();
        this.sweepIntervalNanos = Math.min(this.staleAfterNanos, SWEEP_CAP_NANOS);
    }

    /**
     * Checks whether the trigger is acting.
     *
     * <p>This is the first thing the tick callback reads. {@code false} means the feed costs one flag
     * check and returns; nothing is counted and no unit is called.
     *
     * @return {@code true} when overrun bodies may open windows
     */
    public boolean enabled() {
        return this.enabled && !this.closed;
    }

    /**
     * Registers a work unit.
     *
     * <p>Registration is by instance: registering the same instance twice keeps one entry. A closed
     * trigger refuses new units, so a shutdown path cannot resurrect a window.
     *
     * @param unit unit to register
     * @return {@code true} if the unit is now registered, {@code false} if it already was or the
     *         trigger is closed
     * @throws NullPointerException if {@code unit} is {@code null}
     */
    public boolean register(final ReliefUnit unit) {
        Objects.requireNonNull(unit, "unit");
        if (this.closed) {
            return false;
        }
        return this.units.addIfAbsent(unit);
    }

    /**
     * Removes a previously registered work unit.
     *
     * @param unit unit to remove
     * @return {@code true} if the unit was registered
     * @throws NullPointerException if {@code unit} is {@code null}
     */
    public boolean unregister(final ReliefUnit unit) {
        Objects.requireNonNull(unit, "unit");
        return this.units.remove(unit);
    }

    /**
     * Returns the epoch of the currently open window of one region.
     *
     * <p>A unit may call this from the region thread to tag work with the window it belongs to. Zero
     * means the region has no open window, so the work is not relief work.
     *
     * @param regionId id of the region
     * @return the open window epoch, or zero when the region is calm, unknown or the trigger is closed
     */
    public long windowEpoch(final long regionId) {
        if (this.closed) {
            return 0L;
        }
        final RegionState state = this.regions.get(regionId);
        if (state == null) {
            return 0L;
        }
        synchronized (state) {
            return state.open ? state.epoch : 0L;
        }
    }

    /**
     * Stops the trigger and drops its state.
     *
     * <p>Registered units are dropped without a close callback: this runs while the server shuts down,
     * after the subsystems that own the units are already gone.
     */
    public void close() {
        this.closed = true;
        this.units.clear();
        this.regions.clear();
    }

    /**
     * Observes one executed tick body.
     *
     * <p>Called on the region thread. The policy is driven by the body's own timestamps and by the
     * deadline the scheduler measured it against, never by a clock read of the trigger.
     *
     * @param origin              scheduler that ran the body
     * @param regionId            region of the body, or {@code -1} for a tick without a region
     * @param tickId              id of the scheduled tick
     * @param scheduledStartNanos deadline the body was measured against
     * @param startNanos          moment the body started
     * @param endNanos            moment the body returned
     * @param periodsAhead        whole periods the handle was behind at the body start
     * @param tickCount           periods the handle accounted for in this body
     */
    @Override
    public void onTick(
        final TickOrigin origin,
        final long regionId,
        final long tickId,
        final long scheduledStartNanos,
        final long startNanos,
        final long endNanos,
        final long periodsAhead,
        final long tickCount
    ) {
        if (!this.enabled || this.closed) {
            return;
        }
        if (regionId < 0L) {
            // the global tick has no region and no relief work behind it
            return;
        }
        final long now = endNanos;
        final boolean overrun = this.isOverrun(startNanos, endNanos, scheduledStartNanos, periodsAhead);
        final Thread owner = Thread.currentThread();
        final RegionState state = this.stateFor(regionId, owner, now);
        final Transition transition;
        synchronized (state) {
            transition = state.tick(this, overrun, now);
        }
        this.maybeSweep(now);
        if (transition != null) {
            this.fire(transition);
        }
    }

    /**
     * Returns the counters and gauges of the trigger.
     *
     * @return an immutable snapshot, never {@code null}
     */
    public ReliefTriggerSnapshot snapshot() {
        final long now = this.nanoClock.getAsLong();
        final List<ReliefTriggerSnapshot.RegionOffence> offenders = new ArrayList<>(this.regions.size());
        int open = 0;
        for (final RegionState state : this.regions.values()) {
            synchronized (state) {
                if (state.open) {
                    open++;
                }
                offenders.add(new ReliefTriggerSnapshot.RegionOffence(
                    state.regionId,
                    state.overrunBodies,
                    state.windows,
                    state.open
                ));
            }
        }
        offenders.sort(
            Comparator.comparingLong(ReliefTriggerSnapshot.RegionOffence::overrunBodies).reversed()
                .thenComparingLong(ReliefTriggerSnapshot.RegionOffence::regionId)
        );
        final List<String> ids = new ArrayList<>(this.units.size());
        for (final ReliefUnit unit : this.units) {
            ids.add(safeId(unit));
        }
        final long lastOpened = this.lastWindowOpenedNanos.get();
        final long lastAge = lastOpened == Long.MIN_VALUE ? -1L : Math.max(0L, now - lastOpened);
        return new ReliefTriggerSnapshot(
            this.enabled(),
            ids.size(),
            ids,
            this.regions.size(),
            open,
            this.windowsEntered.sum(),
            this.windowsExited.sum(),
            this.windowsAbandoned.sum(),
            this.overrunBodies.sum(),
            this.calmBodies.sum(),
            this.suppressedByCooldown.sum(),
            this.suppressedByRate.sum(),
            this.unitFailures.sum(),
            lastAge,
            offenders.size() > TOP_OFFENDERS ? offenders.subList(0, TOP_OFFENDERS) : offenders
        );
    }

    private static String safeId(final ReliefUnit unit) {
        try {
            return unit.id();
        } catch (Throwable _) {
            // a broken unit must not break the command either
            return unit.getClass().getSimpleName();
        }
    }

    private boolean isOverrun(
        final long startNanos,
        final long endNanos,
        final long scheduledStartNanos,
        final long periodsAhead
    ) {
        if (endNanos - startNanos >= this.overrunNanos) {
            return true;
        }
        if (periodsAhead > 1L) {
            return true;
        }
        return startNanos - scheduledStartNanos >= this.overrunNanos;
    }

    private RegionState stateFor(final long regionId, final Thread owner, final long now) {
        final RegionState existing = this.regions.get(regionId);
        if (existing != null && existing.owner == owner) {
            return existing;
        }
        final RegionState fresh = new RegionState(regionId, owner, now);
        final RegionState previous = this.regions.put(regionId, fresh);
        if (previous != null) {
            // the id now belongs to a different live thread: a new region incarnation, not a calm one
            this.abandon(previous, now);
        }
        return fresh;
    }

    private void abandon(final RegionState state, final long now) {
        synchronized (state) {
            if (!state.open) {
                return;
            }
            state.open = false;
            state.lastClosedAtNanos = now;
        }
        this.windowsAbandoned.increment();
    }

    private void maybeSweep(final long now) {
        final long last = this.lastSweepNanos.get();
        if (last != 0L && now - last < this.sweepIntervalNanos) {
            return;
        }
        if (!this.lastSweepNanos.compareAndSet(last, now)) {
            return;
        }
        for (final Map.Entry<Long, RegionState> entry : this.regions.entrySet()) {
            final RegionState state = entry.getValue();
            final boolean stale;
            synchronized (state) {
                stale = now - state.lastTickNanos >= this.staleAfterNanos;
            }
            if (!stale) {
                continue;
            }
            if (this.regions.remove(entry.getKey(), state)) {
                this.abandon(state, now);
            }
        }
    }

    private void fire(final Transition transition) {
        for (final ReliefUnit unit : this.units) {
            try {
                if (transition.opened) {
                    unit.onWindowOpen(transition.window);
                } else {
                    unit.onWindowClose(transition.window);
                }
            } catch (final Throwable failure) {
                this.unitFailures.increment();
                LOGGER.warn("A region relief unit failed in a window callback; it is isolated and the tick continues", failure);
            }
        }
    }

    /**
     * The decision of one observed body: nothing, a window opening, or a window closing.
     */
    private static final class Transition {

        private final boolean opened;
        private final ReliefWindow window;

        private Transition(final boolean opened, final ReliefWindow window) {
            this.opened = opened;
            this.window = window;
        }
    }

    /**
     * Counters of one region. Only the region thread and the sweeper touch it, and only under the
     * instance monitor.
     */
    private static final class RegionState {

        private final long regionId;
        private final Thread owner;

        private long epoch;
        private int overrunStreak;
        private int calmStreak;
        private boolean open;
        private long openedAtNanos;
        private long lastClosedAtNanos;
        private long lastTickNanos;
        private long windows;
        private long overrunBodies;
        private long calmBodies;
        private long rateWindowStartNanos;
        private int windowsInRateWindow;
        private boolean rateWindowStarted;

        private RegionState(final long regionId, final Thread owner, final long now) {
            this.regionId = regionId;
            this.owner = owner;
            // a state is never born stale: the sweeper must not take it before its first body is counted
            this.lastTickNanos = now;
        }

        private Transition tick(final ReliefTrigger trigger, final boolean overrun, final long now) {
            this.lastTickNanos = now;
            if (overrun) {
                this.overrunStreak++;
                this.calmStreak = 0;
                this.overrunBodies++;
                trigger.overrunBodies.increment();
            } else {
                this.calmStreak++;
                this.overrunStreak = 0;
                this.calmBodies++;
                trigger.calmBodies.increment();
            }

            if (this.open) {
                if (this.calmStreak < trigger.exitCalm) {
                    return null;
                }
                this.open = false;
                this.lastClosedAtNanos = now;
                this.overrunStreak = 0;
                trigger.windowsExited.increment();
                return new Transition(false, this.window());
            }

            if (this.overrunStreak < trigger.enterOverruns) {
                return null;
            }
            if (this.windows > 0L && now - this.lastClosedAtNanos < trigger.cooldownNanos) {
                trigger.suppressedByCooldown.increment();
                return null;
            }
            if (!this.rateAllows(trigger, now)) {
                trigger.suppressedByRate.increment();
                return null;
            }
            this.open = true;
            this.openedAtNanos = now;
            this.epoch++;
            this.windows++;
            this.windowsInRateWindow++;
            trigger.windowsEntered.increment();
            trigger.lastWindowOpenedNanos.set(now);
            return new Transition(true, this.window());
        }

        private boolean rateAllows(final ReliefTrigger trigger, final long now) {
            if (!this.rateWindowStarted || now - this.rateWindowStartNanos >= trigger.rateWindowNanos) {
                this.rateWindowStarted = true;
                this.rateWindowStartNanos = now;
                this.windowsInRateWindow = 0;
            }
            return this.windowsInRateWindow < trigger.maxWindows;
        }

        private ReliefWindow window() {
            return new ReliefWindow(this.regionId, this.owner, this.epoch, this.openedAtNanos);
        }
    }
}
