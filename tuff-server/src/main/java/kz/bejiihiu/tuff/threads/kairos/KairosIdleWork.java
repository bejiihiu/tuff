package kz.bejiihiu.tuff.threads.kairos;

import java.util.concurrent.TimeUnit;

/**
 * Knobs of the idle path: what the fund may run while a tick thread has nothing to tick.
 *
 * <p>Idle work is off by default and has two independent sources. {@code midTick} lets a runner that
 * already holds a region's permit drain that region's intermediate tasks while it waits for the
 * deadline; {@code utmiJobs} lets a runner with no permit at all take a job from an
 * {@link kz.bejiihiu.tuff.threads.idle.IdleWorkSource}. {@code enabled} is the master switch: with it
 * off neither source is touched, whatever the other two say.
 *
 * <p>The limits are the self-protection. {@code sliceMillis} is the most wall clock one piece of idle
 * work may take before the runner goes back to waiting; {@code maxAddedLagMillis} is the most lateness
 * a tick may suffer because of that work before the whole idle path disables itself; and
 * {@code maxSharePercent} caps how much of a one second window may be spent on idle work, so a fund
 * cannot be kept busy warming itself while it has real ticks to run.
 *
 * <p>Read once when the scheduler starts, like every other Kairos knob. Values are validated here so a
 * typo cannot reach a runner.
 *
 * @param enabled            master switch of the idle path
 * @param midTick            whether a waiting runner may drain its region's intermediate tasks
 * @param utmiJobs           whether a parked runner may take jobs from an idle-safe source
 * @param sliceMillis        wall clock one idle slice may take, in milliseconds
 * @param maxAddedLagMillis  lateness a tick may suffer from idle work before the path disables itself
 * @param maxSharePercent    share of a one second window idle work may spend, from 0 to 100
 */
public record KairosIdleWork(
    boolean enabled,
    boolean midTick,
    boolean utmiJobs,
    int sliceMillis,
    int maxAddedLagMillis,
    int maxSharePercent
) {

    /**
     * Validates the knobs.
     *
     * @throws IllegalArgumentException if a value is outside its documented range
     */
    public KairosIdleWork {
        if (sliceMillis < 1 || sliceMillis > 100) {
            throw new IllegalArgumentException("sliceMillis must be in [1, 100], got " + sliceMillis);
        }
        if (maxAddedLagMillis < 0 || maxAddedLagMillis > 10_000) {
            throw new IllegalArgumentException(
                "maxAddedLagMillis must be in [0, 10000], got " + maxAddedLagMillis
            );
        }
        if (maxSharePercent < 0 || maxSharePercent > 100) {
            throw new IllegalArgumentException(
                "maxSharePercent must be in [0, 100], got " + maxSharePercent
            );
        }
    }

    /**
     * Returns the safe default: the whole idle path is off, with the documented limits already set.
     *
     * @return the idle knobs a server that never opted in gets, never {@code null}
     */
    public static KairosIdleWork off() {
        return new KairosIdleWork(false, false, false, 1, 5, 25);
    }

    /**
     * Returns whether at least one source would actually run under these knobs.
     *
     * @return {@code true} when the master switch is on and a source is selected
     */
    public boolean armed() {
        return this.enabled && (this.midTick || this.utmiJobs);
    }

    /**
     * Returns the slice in nanoseconds.
     *
     * @return the configured slice, converted
     */
    public long sliceNanos() {
        return TimeUnit.MILLISECONDS.toNanos(this.sliceMillis);
    }

    /**
     * Returns the added lag budget in nanoseconds.
     *
     * @return the configured budget, converted
     */
    public long maxAddedLagNanos() {
        return TimeUnit.MILLISECONDS.toNanos(this.maxAddedLagMillis);
    }
}
