package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a worker starts a task.
 *
 * <p>The priority field is the effective priority at dispatch time, which can differ from the
 * submitted one when aging lifted the task.
 */
@Name("tuff.compute.TaskStarted")
@Label("Compute Task Started")
@Description("A worker started executing a compute task")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskStartedEvent extends Event {

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
     * Declared category of the task.
     */
    @Label("Task Type")
    public String taskType;

    /**
     * Effective priority at dispatch time.
     */
    @Label("Priority")
    public String priority;

    /**
     * How long the task waited in the queue.
     */
    @Label("Queue Wait")
    public long queueWaitNanos;

    /**
     * Tasks of the same consumer running right after this one started.
     */
    @Label("Active Tasks Of Consumer")
    public int activeTasks;

    /**
     * Human readable task label, empty when the submission had none.
     */
    @Label("Task Label")
    public String taskLabel;
}
