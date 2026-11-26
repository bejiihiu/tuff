package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A thread was created by, or adopted into, a pool.
 *
 * <p>Emitted on the creating thread. The fields are all known at that moment, so nothing has to be
 * looked up later; a recording therefore shows the pool a thread was born into even if it dies before
 * the next sample.
 */
@Name("tuff.threads.ThreadCreated")
@Label("Thread Created")
@Description("A thread was created or adopted by a managed pool")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class ThreadCreatedEvent extends Event {

    /**
     * JVM thread id.
     */
    @Label("Thread ID")
    public long threadId;

    /**
     * Thread name at creation time.
     */
    @Label("Thread Name")
    public String threadName;

    /**
     * Pool that owns the thread.
     */
    @Label("Pool")
    public String poolId;

    /**
     * Whether the thread is a daemon thread.
     */
    @Label("Daemon")
    public boolean daemon;

    /**
     * Java priority the thread was started with.
     */
    @Label("Priority")
    public int priority;
}
