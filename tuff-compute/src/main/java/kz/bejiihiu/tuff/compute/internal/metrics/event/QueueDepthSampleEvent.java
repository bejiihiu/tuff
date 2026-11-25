package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Periodic gauge of the queue depth, emitted from the scheduler's sample hook.
 *
 * <p>The event is deliberately not self throttling: the sampler that feeds it already runs once per
 * configured interval, and a second hidden rate limit would make the sampled series lie about short
 * bursts.
 */
@Name("tuff.compute.QueueDepthSample")
@Label("Compute Queue Depth")
@Description("Periodic sample of the compute queue depth")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class QueueDepthSampleEvent extends Event {

    /**
     * Queued tasks at the sample moment.
     */
    @Label("Queued Tasks")
    public int queuedTasks;

    /**
     * Configured queue capacity at the sample moment.
     */
    @Label("Capacity")
    public int capacity;

    /**
     * Queued tasks divided by capacity.
     */
    @Label("Fill Ratio")
    public double fillRatio;
}
