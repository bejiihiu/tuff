package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a task finished successfully.
 */
@Name("tuff.compute.TaskCompleted")
@Label("Compute Task Completed")
@Description("A compute task finished successfully")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskCompletedEvent extends Event {

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
     * How long the task body ran.
     */
    @Label("Execution Time")
    public long executionNanos;

    /**
     * Human readable task label, empty when the submission had none.
     */
    @Label("Task Label")
    public String taskLabel;
}
