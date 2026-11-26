package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A lease request was not granted before its patience ran out.
 *
 * <p>The frozen sink contract has no hook for the pool side of this, so the event is emitted by the
 * sink's own {@code leaseTimedOut} method, which the controller calls on the path that already throws
 * {@code LeaseTimeoutException}. It is worth an event: "who asked and how long were they willing to
 * wait" is the cheapest evidence of a starving pool.
 */
@Name("tuff.threads.LeaseTimeout")
@Label("Lease Timed Out")
@Description("A lease request was not granted within its maximum wait")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class LeaseTimeoutEvent extends Event {

    /**
     * Pool the capacity was requested from.
     */
    @Label("Pool")
    public String poolId;

    /**
     * Absolute deadline of the request on the caller's monotonic timeline.
     */
    @Label("Deadline (ns)")
    public long deadlineNanos;

    /**
     * How long the caller was willing to block, in nanoseconds.
     */
    @Label("Max Wait (ns)")
    public long maxWaitNanos;
}
