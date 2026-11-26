package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A thread exists in the JVM but is not accounted to any pool.
 *
 * <p>One event per thread per scan. The scan is a command or startup activity, so a leak shows up as a
 * burst of events with the stack the thread had at scan time in the surrounding recording, not as a
 * per second stream that would drown the recording.
 */
@Name("tuff.threads.UnknownThreads")
@Label("Unknown Thread")
@Description("A thread is not accounted to any registered pool")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class UnknownThreadsEvent extends Event {

    /**
     * JVM thread id.
     */
    @Label("Thread ID")
    public long threadId;

    /**
     * Thread name at scan time.
     */
    @Label("Thread Name")
    public String threadName;
}
