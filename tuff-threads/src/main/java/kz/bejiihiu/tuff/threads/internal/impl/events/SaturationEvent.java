package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A pool entered, left or stayed in a pressure level.
 *
 * <p>Emitted on a level change and once per sampling window while the pool is saturated, so a
 * recording contains a rising edge plus a heartbeat instead of one event per second forever.
 */
@Name("tuff.threads.Saturation")
@Label("Pool Saturation")
@Description("A pool changed its saturation level or stayed saturated for a full window")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class SaturationEvent extends Event {

    /**
     * Pool the report belongs to.
     */
    @Label("Pool")
    public String poolId;

    /**
     * Summary level: {@code UNKNOWN}, {@code IDLE}, {@code NORMAL}, {@code BUSY} or {@code SATURATED}.
     */
    @Label("Level")
    public String level;

    /**
     * Share of the pool's capacity in use.
     */
    @Label("Utilisation")
    public double utilisation;

    /**
     * Share of the queue capacity in use.
     */
    @Label("Queue Pressure")
    public double queuePressure;

    /**
     * Tasks waiting at capture time.
     */
    @Label("Queued Tasks")
    public long queuedTasks;

    /**
     * Age of the oldest queued task, zero when nothing waits.
     */
    @Label("Longest Wait (ns)")
    public long longestWaitNanos;
}
