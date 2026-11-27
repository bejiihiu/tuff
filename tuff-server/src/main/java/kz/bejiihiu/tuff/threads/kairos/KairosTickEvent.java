package kz.bejiihiu.tuff.threads.kairos;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * One tick body the fund executed, with the deadline it was measured against.
 *
 * <p>This is the event that turns "the region looked slow in a five second average" into a real hard
 * miss: {@code scheduledStartNanos} and {@code startNanos} give the lag, {@code missedPeriods} gives
 * how many whole periods the handle was behind before the body, and {@code skipped} gives how many of
 * them the body wrote off instead of running. A stand can therefore count ticks that missed their
 * deadline and periods that were dropped, without guessing from a TPS sample.
 *
 * <p>Armed by {@code kairos.metrics.jfr} on top of the recording's own filter: the switch is read
 * before the event is even constructed, so a server that does not record pays one volatile read per
 * tick. All timestamps are {@link System#nanoTime()}.
 */
@Name("tuff.scheduler.Tick")
@Label("Kairos Tick")
@Description("A region tick body executed by the Kairos fund")
@Category({"Tuff", "Scheduler"})
@StackTrace(false)
public final class KairosTickEvent extends Event {

    /**
     * Id of the region, or {@code -1} when the tick is not a region handle.
     */
    @Label("Region Id")
    public long regionId;

    /**
     * Id of the scheduled tick, the same value the queue orders ties by.
     */
    @Label("Tick Id")
    public long tickId;

    /**
     * Deadline the tick was measured against, in {@link System#nanoTime()} terms.
     */
    @Label("Scheduled Start (ns)")
    public long scheduledStartNanos;

    /**
     * Moment the body started, in {@link System#nanoTime()} terms.
     */
    @Label("Start (ns)")
    public long startNanos;

    /**
     * Moment the body returned, in {@link System#nanoTime()} terms.
     */
    @Label("End (ns)")
    public long endNanos;

    /**
     * Whole periods the handle was behind before the body, or {@code -1} when it could not answer.
     */
    @Label("Missed Periods")
    public long missedPeriods;

    /**
     * {@code 1} when this body accounted for a single period to recover a backlog, otherwise {@code 0}.
     */
    @Label("Catch Up Ticks")
    public int catchUpTicks;

    /**
     * Periods this body wrote off instead of running, or {@code -1} when unknown.
     */
    @Label("Skipped Periods")
    public long skipped;

    /**
     * Wall clock the body spent, in nanoseconds.
     */
    @Label("Executed (ns)")
    public long executedNanos;

    /**
     * Emits the event when a recording has it enabled.
     *
     * @param regionId                       region of the tick, or {@code -1}
     * @param tickId                         id of the scheduled tick
     * @param scheduledStartNanos            deadline the tick was measured against
     * @param startNanos                     moment the body started
     * @param endNanos                       moment the body returned
     * @param missedPeriods                  periods the handle was behind, or {@code -1}
     * @param catchUpTicks                   one for a catch-up body, otherwise zero
     * @param skipped                        periods written off, or {@code -1}
     */
    static void emit(
        final long regionId,
        final long tickId,
        final long scheduledStartNanos,
        final long startNanos,
        final long endNanos,
        final long missedPeriods,
        final int catchUpTicks,
        final long skipped
    ) {
        try {
            if (!FlightRecorder.isInitialized()) {
                // no recording was ever started, so there is not even a dormant event to fill
                return;
            }
            final KairosTickEvent event = new KairosTickEvent();
            if (!event.isEnabled()) {
                return;
            }
            event.regionId = regionId;
            event.tickId = tickId;
            event.scheduledStartNanos = scheduledStartNanos;
            event.startNanos = startNanos;
            event.endNanos = endNanos;
            event.missedPeriods = missedPeriods;
            event.catchUpTicks = catchUpTicks;
            event.skipped = skipped;
            event.executedNanos = endNanos - startNanos;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a tick
        }
    }
}
