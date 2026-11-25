package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Periodic gauge of the worker pool, emitted from the scheduler's sample hook.
 */
@Name("tuff.compute.WorkerPoolState")
@Label("Compute Worker Pool State")
@Description("Periodic sample of worker counts and the concurrency limit")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class WorkerPoolStateEvent extends Event {

    /**
     * Live worker threads.
     */
    @Label("Total Workers")
    public int totalWorkers;

    /**
     * Workers executing a task.
     */
    @Label("Active Workers")
    public int activeWorkers;

    /**
     * Workers waiting for a task.
     */
    @Label("Idle Workers")
    public int idleWorkers;

    /**
     * Pool wide concurrency limit.
     */
    @Label("Concurrency Limit")
    public int concurrencyLimit;
}
