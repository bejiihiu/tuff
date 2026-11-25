package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when the number of live workers changes, for example after an autoscaler step.
 *
 * <p>The periodic {@code WorkerPoolState} event shows the steady state; this one marks the transition
 * so a recording can be read without diffing samples by hand.
 */
@Name("tuff.compute.WorkerPoolResized")
@Label("Compute Worker Pool Resized")
@Description("The compute worker pool changed its number of live workers")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class WorkerPoolResizedEvent extends Event {

    /**
     * Live worker threads after the change.
     */
    @Label("Total Workers")
    public int totalWorkers;

    /**
     * Concurrency limit in effect after the change.
     */
    @Label("Concurrency Limit")
    public int concurrencyLimit;
}
