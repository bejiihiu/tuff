package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a task was cancelled because it waited longer than its consumer policy allows.
 *
 * <p>This is a dedicated event instead of a {@code TaskCancelled} with a reason field because queue
 * expiry is the scheduler's own admission control working as designed, and it is the first thing to
 * look at when a consumer mysteriously loses work under load.
 */
@Name("tuff.compute.TaskQueueExpired")
@Label("Compute Task Queue Expired")
@Description("A queued compute task was cancelled because its maximum queue wait elapsed")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskQueueExpiredEvent extends Event {

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
     * Human readable task label, empty when the submission had none.
     */
    @Label("Task Label")
    public String taskLabel;
}
