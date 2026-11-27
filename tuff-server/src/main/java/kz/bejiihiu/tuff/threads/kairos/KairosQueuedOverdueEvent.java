package kz.bejiihiu.tuff.threads.kairos;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A tick stayed in the wait queue past the overdue threshold while the fund could not serve it.
 *
 * <p>Emitted only when the rate limited warning fires, never once per pass: the recording then holds
 * the edges an operator lines up with a TPS graph instead of a flood that hides them. The cluster of
 * fields is the same one the warning prints, so a recording explains itself without the log.
 *
 * <p>JFR is off unless a recording asks for this event, so the object is cheap on a server that never
 * profiles; the {@code isEnabled} check still guards the field writes.
 */
@Name("tuff.scheduler.QueuedOverdue")
@Label("Kairos Queued Tick Overdue")
@Description("A tick waited in the Kairos queue past the overdue threshold")
@Category({"Tuff", "Scheduler"})
@StackTrace(false)
public final class KairosQueuedOverdueEvent extends Event {

    /**
     * Id of the region whose tick waited longest, or {@code -1} when the tick was not a region handle.
     */
    @Label("Region Id")
    public long regionId;

    /**
     * How long the oldest overdue tick has been past its deadline, in milliseconds.
     */
    @Label("Oldest Age (ms)")
    public long ageMillis;

    /**
     * Queued ticks past the threshold at the moment of the warning.
     */
    @Label("Overdue Ticks")
    public int overdueTicks;

    /**
     * Live tick threads when the warning fired.
     */
    @Label("Live Threads")
    public int liveThreads;

    /**
     * Ceiling the fund is allowed to reach.
     */
    @Label("Max Threads")
    public int maxThreads;

    /**
     * {@code true} when the fund was already at its ceiling, so growing cannot help.
     */
    @Label("Saturated")
    public boolean saturated;

    /**
     * Emits the event when a recording has it enabled.
     *
     * @param regionId     region of the oldest overdue tick, or {@code -1}
     * @param ageMillis    its age past the deadline
     * @param overdueTicks number of overdue ticks
     * @param liveThreads  live runners
     * @param maxThreads   fund ceiling
     * @param saturated    whether the fund was at the ceiling
     */
    static void emit(
        final long regionId,
        final long ageMillis,
        final int overdueTicks,
        final int liveThreads,
        final int maxThreads,
        final boolean saturated
    ) {
        try {
            if (!FlightRecorder.isInitialized()) {
                // no recording was ever started, so there is not even a dormant event to fill
                return;
            }
            final KairosQueuedOverdueEvent event = new KairosQueuedOverdueEvent();
            if (!event.isEnabled()) {
                return;
            }
            event.regionId = regionId;
            event.ageMillis = ageMillis;
            event.overdueTicks = overdueTicks;
            event.liveThreads = liveThreads;
            event.maxThreads = maxThreads;
            event.saturated = saturated;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break the supervisor
        }
    }
}
