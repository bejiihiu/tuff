package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when a submission was refused by admission control.
 *
 * <p>Rejected submissions never become tasks, so this event carries no task id.
 */
@Name("tuff.compute.TaskRejected")
@Label("Compute Task Rejected")
@Description("A compute submission was refused instead of being queued")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class TaskRejectedEvent extends Event {

    /**
     * Consumer the submission was made for.
     */
    @Label("Consumer")
    public String consumerId;

    /**
     * Why the submission was refused.
     */
    @Label("Reason")
    public String reason;
}
