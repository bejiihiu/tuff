package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a task starts on the virtual lane.
 *
 * <p>{@code spilled} tells whether the task reached the lane because the platform pool was saturated
 * (a NORMAL task) or because its priority owns the lane (a LOW task). The running and limit fields
 * are the lane's own gauges, not the platform pool's.
 */
@Name("tuff.compute.VirtualTask")
@Label("Compute Virtual Task")
@Description("A virtual thread started executing a compute task")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class VirtualTaskEvent extends Event {

    /**
     * Identifier assigned by the scheduler.
     */
    @Label("Task ID")
    public long taskId;

    /**
     * Consumer the task belongs to.
     */
    @Label("Consumer")
    public String consumerId;

    /**
     * Submitted priority of the task.
     */
    @Label("Priority")
    public String priority;

    /**
     * How long the task waited before the virtual lane claimed it.
     */
    @Label("Queue Wait")
    public long queueWaitNanos;

    /**
     * Whether the task was spilled from the platform lane.
     */
    @Label("Spilled")
    public boolean spilled;

    /**
     * Virtual tasks running right after this one started.
     */
    @Label("Virtual Running")
    public int running;

    /**
     * Hard concurrency limit of the virtual lane.
     */
    @Label("Virtual Limit")
    public int limit;

    /**
     * Human readable task label, empty when the submission had none.
     */
    @Label("Task Label")
    public String taskLabel;
}
