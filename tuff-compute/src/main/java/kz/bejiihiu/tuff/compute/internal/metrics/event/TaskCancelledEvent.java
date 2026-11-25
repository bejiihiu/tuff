package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a task was cancelled for any reason other than its queue wait expiring.
 */
@Name("tuff.compute.TaskCancelled")
@Label("Compute Task Cancelled")
@Description("A compute task was cancelled before producing a result")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskCancelledEvent extends Event {

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
     * Why the task was cancelled.
     */
    @Label("Reason")
    public String reason;

    /**
     * Human readable task label, empty when the submission had none.
     */
    @Label("Task Label")
    public String taskLabel;
}
