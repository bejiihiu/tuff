package kz.bejiihiu.tuff.threads.observe;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * One executed tick body, with the deadline it was measured against.
 *
 * <p>The event turns "the region looked slow in a five second average" into a real hard miss:
 * {@code scheduledStartNanos} against {@code startNanos} is the lag of that exact body,
 * {@code missedPeriods} is how many whole periods the handle was behind before it ran (one is on time),
 * and {@code skipped} is how many of those periods the body wrote off instead of running. The recorder
 * runs from the region handle for every scheduler, so EDF, work stealing and Kairos produce the same
 * rows.
 *
 * <p>Armed by {@code thread-management.region-execution.metrics.jfr} on top of the recording's own
 * filter: the switch is read before the event is constructed, so a server that does not record pays one
 * volatile read per tick. All timestamps are {@link System#nanoTime()}, and the deadline is the value
 * the handle held before it advanced the schedule.
 */
@Name("tuff.scheduler.Tick")
@Label("Scheduler Tick")
@Description("A region tick body and the deadline it was measured against")
@Category({"Tuff", "Scheduler"})
@StackTrace(false)
public final class TickEvent extends Event {

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
     * Scheduler that ran the body.
     */
    @Label("Origin")
    public String origin;

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
     * Whole periods the handle was behind before the body; one is on time.
     */
    @Label("Periods Ahead")
    public long missedPeriods;

    /**
     * {@code 1} when this body accounted for a single period to recover a backlog, otherwise {@code 0}.
     */
    @Label("Catch Up Ticks")
    public int catchUpTicks;

    /**
     * Periods this body wrote off instead of running.
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
     * @param origin               scheduler that ran the body
     * @param regionId             region of the tick, or {@code -1}
     * @param tickId               id of the scheduled tick
     * @param scheduledStartNanos  deadline the tick was measured against
     * @param startNanos           moment the body started
     * @param endNanos             moment the body returned
     * @param periodsAhead         periods the handle was behind at the body start
     * @param tickCount            periods the handle accounted for in this body
     */
    static void emit(
        final TickOrigin origin,
        final long regionId,
        final long tickId,
        final long scheduledStartNanos,
        final long startNanos,
        final long endNanos,
        final long periodsAhead,
        final long tickCount
    ) {
        try {
            if (!FlightRecorder.isInitialized()) {
                // no recording was ever started, so there is not even a dormant event to fill
                return;
            }
            final TickEvent event = new TickEvent();
            if (!event.isEnabled()) {
                return;
            }
            event.regionId = regionId;
            event.tickId = tickId;
            event.origin = origin.name();
            event.scheduledStartNanos = scheduledStartNanos;
            event.startNanos = startNanos;
            event.endNanos = endNanos;
            event.missedPeriods = periodsAhead;
            event.catchUpTicks = periodsAhead > 1L && tickCount == 1L ? 1 : 0;
            event.skipped = Math.max(0L, tickCount - 1L);
            event.executedNanos = endNanos - startNanos;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break a tick
        }
    }
}
