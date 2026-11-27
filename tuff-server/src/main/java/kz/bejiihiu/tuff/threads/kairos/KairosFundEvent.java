package kz.bejiihiu.tuff.threads.kairos;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * The fund as one supervisor pass saw it.
 *
 * <p>One event per pass, so a recording holds the fund's own timeline - min, live and target, what the
 * queues held, how many ticks were already past the threshold, and the lifetime spawn and retire
 * counters - next to the per-tick events. That is what a stand needs to explain a hard miss: was the
 * permit missing because the fund had no headroom, or because a tick was slow?
 *
 * <p>Armed by {@code region-execution.kairos.metrics.jfr} on top of the recording's own filter. The
 * per-tick event belongs to the region handle and has a separate switch.
 */
@Name("tuff.scheduler.Fund")
@Label("Kairos Fund")
@Description("One Kairos supervisor pass and the state of the fund it observed")
@Category({"Tuff", "Scheduler"})
@StackTrace(false)
public final class KairosFundEvent extends Event {

    /**
     * Floor of the fund.
     */
    @Label("Min Threads")
    public int minThreads;

    /**
     * Runners started and not yet exited.
     */
    @Label("Live Threads")
    public int liveThreads;

    /**
     * Size the pass asked for.
     */
    @Label("Target Threads")
    public int targetThreads;

    /**
     * Ceiling the fund may reach.
     */
    @Label("Max Threads")
    public int maxThreads;

    /**
     * Ticks waiting for a permit, in the shared queue and in runner backlogs.
     */
    @Label("Queued Ticks")
    public int queuedTicks;

    /**
     * Runners holding a permit and waiting for their deadline.
     */
    @Label("Awaiting Ticks")
    public int awaitingTicks;

    /**
     * Runners inside a tick body.
     */
    @Label("Executing Ticks")
    public int executingTicks;

    /**
     * Waiting ticks past the derived threshold at the moment of the pass.
     */
    @Label("Overdue Ticks")
    public int overdueTicks;

    /**
     * Runners started since the process began.
     */
    @Label("Spawned")
    public long spawned;

    /**
     * Runners retired since the process began.
     */
    @Label("Retired")
    public long retired;

    /**
     * Emits the event when a recording has it enabled.
     *
     * @param minThreads     floor of the fund
     * @param liveThreads    runners started and not yet exited
     * @param targetThreads  size the pass asked for
     * @param maxThreads     ceiling of the fund
     * @param queuedTicks    ticks waiting for a permit
     * @param awaitingTicks  runners waiting for their deadline
     * @param executingTicks runners inside a body
     * @param overdueTicks   waiting ticks past the threshold
     * @param spawned        lifetime spawn counter
     * @param retired        lifetime retire counter
     */
    static void emit(
        final int minThreads,
        final int liveThreads,
        final int targetThreads,
        final int maxThreads,
        final int queuedTicks,
        final int awaitingTicks,
        final int executingTicks,
        final int overdueTicks,
        final long spawned,
        final long retired
    ) {
        try {
            if (!FlightRecorder.isInitialized()) {
                // no recording was ever started, so there is not even a dormant event to fill
                return;
            }
            final KairosFundEvent event = new KairosFundEvent();
            if (!event.isEnabled()) {
                return;
            }
            event.minThreads = minThreads;
            event.liveThreads = liveThreads;
            event.targetThreads = targetThreads;
            event.maxThreads = maxThreads;
            event.queuedTicks = queuedTicks;
            event.awaitingTicks = awaitingTicks;
            event.executingTicks = executingTicks;
            event.overdueTicks = overdueTicks;
            event.spawned = spawned;
            event.retired = retired;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break the supervisor
        }
    }
}
