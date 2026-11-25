package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a submission was accepted into the queue.
 *
 * <p>The queue depth is the depth right after this task was counted, which lets a recording show the
 * backlog a submission ran into.
 */
@Name("tuff.compute.TaskSubmitted")
@Label("Compute Task Submitted")
@Description("A compute task was accepted into the scheduler queue")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskSubmittedEvent extends Event {

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
     * Submitted priority.
     */
    @Label("Priority")
    public String priority;

    /**
     * Queue depth right after the task was accepted.
     */
    @Label("Queue Depth")
    public int queueDepth;
}
