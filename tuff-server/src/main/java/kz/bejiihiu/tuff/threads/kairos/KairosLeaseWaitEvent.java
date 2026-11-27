package kz.bejiihiu.tuff.threads.kairos;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A permit was handed out and the tick still had to wait longer than the configured lease timeout.
 *
 * <p>Emitted only when the rate limited warning fires, so a recording holds the edges an operator
 * lines up with a TPS graph instead of one event per tick. Two waits are carried, because they
 * point at different causes: {@code queuedMillis} is the wait before a permit existed (too few tick
 * threads, the R5 signal), {@code parkedMillis} is the wait after it existed (the runner was parked
 * on its deadline or lost the CPU).
 *
 * <p>JFR is off unless a recording asks for this event, so the object is cheap on a server that never
 * profiles; the {@code isEnabled} check still guards the field writes.
 */
@Name("tuff.scheduler.LeaseWait")
@Label("Kairos Lease Wait Timeout")
@Description("A tick waited for its Kairos permit longer than the configured lease timeout")
@Category({"Tuff", "Scheduler"})
@StackTrace(false)
public final class KairosLeaseWaitEvent extends Event {

    /**
     * Longest wait between the tick deadline and the permit grant, in milliseconds.
     */
    @Label("Queued Wait (ms)")
    public long queuedMillis;

    /**
     * Longest wait between the permit grant and the tick start, in milliseconds.
     */
    @Label("Parked Wait (ms)")
    public long parkedMillis;

    /**
     * Permits handed out since the process began.
     */
    @Label("Grants")
    public long grants;

    /**
     * Grants whose wait exceeded the timeout since the process began.
     */
    @Label("Timeouts")
    public long timeouts;

    /**
     * Configured timeout that was exceeded, in milliseconds.
     */
    @Label("Timeout (ms)")
    public long timeoutMillis;

    /**
     * Live tick threads when the warning fired.
     */
    @Label("Live Threads")
    public int liveThreads;

    /**
     * Emits the event when a recording has it enabled.
     *
     * @param queuedMillis  longest queued wait of the moment
     * @param parkedMillis  longest parked wait of the moment
     * @param grants        permits handed out so far
     * @param timeouts      timeouts counted so far
     * @param timeoutMillis configured timeout
     * @param liveThreads   live runners
     */
    static void emit(
        final long queuedMillis,
        final long parkedMillis,
        final long grants,
        final long timeouts,
        final long timeoutMillis,
        final int liveThreads
    ) {
        try {
            if (!FlightRecorder.isInitialized()) {
                // no recording was ever started, so there is not even a dormant event to fill
                return;
            }
            final KairosLeaseWaitEvent event = new KairosLeaseWaitEvent();
            if (!event.isEnabled()) {
                return;
            }
            event.queuedMillis = queuedMillis;
            event.parkedMillis = parkedMillis;
            event.grants = grants;
            event.timeouts = timeouts;
            event.timeoutMillis = timeoutMillis;
            event.liveThreads = liveThreads;
            event.commit();
        } catch (Throwable _) {
            // profiling must never break the supervisor
        }
    }
}
