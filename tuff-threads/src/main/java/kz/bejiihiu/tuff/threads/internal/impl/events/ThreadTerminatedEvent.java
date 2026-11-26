package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A registered thread reached the end of its body.
 *
 * <p>The CPU time is the last value the JVM reported before the thread went away, or {@code -1} when
 * the runtime cannot measure per thread CPU at all. It is kept because "this worker burned 40 seconds
 * and then exited" is exactly the question a leak or churn report asks.
 */
@Name("tuff.threads.ThreadTerminated")
@Label("Thread Terminated")
@Description("A thread owned by a managed pool terminated")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class ThreadTerminatedEvent extends Event {

    /**
     * JVM thread id.
     */
    @Label("Thread ID")
    public long threadId;

    /**
     * Thread name at termination time.
     */
    @Label("Thread Name")
    public String threadName;

    /**
     * Pool that owned the thread.
     */
    @Label("Pool")
    public String poolId;

    /**
     * State observed at termination.
     */
    @Label("State")
    public String state;

    /**
     * Total CPU time consumed by the thread, or {@code -1} when unknown.
     */
    @Label("CPU Time (ns)")
    public long cpuTimeNanos;
}
