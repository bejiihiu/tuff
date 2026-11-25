package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a task finished with a throwable.
 *
 * <p>Only the throwable class and message are recorded: the full stack trace belongs to the caller's
 * own logging, and attaching it here would make the event expensive exactly when the scheduler is
 * already unhappy.
 */
@Name("tuff.compute.TaskFailed")
@Label("Compute Task Failed")
@Description("A compute task finished with a throwable")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskFailedEvent extends Event {

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
     * How long the task body ran before failing.
     */
    @Label("Execution Time")
    public long executionNanos;

    /**
     * Class name of the thrown throwable.
     */
    @Label("Failure Type")
    public String failureType;

    /**
     * Message of the thrown throwable, if it had one.
     */
    @Label("Failure Message")
    public String failureMessage;
}
