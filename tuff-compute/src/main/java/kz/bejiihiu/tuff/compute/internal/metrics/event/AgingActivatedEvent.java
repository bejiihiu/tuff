package kz.bejiihiu.tuff.compute.internal.metrics.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted when priority aging lifts a waiting task, which is the signal that low priority work is
 * being starved by higher priority submissions.
 *
 * <p>The scheduler contract has no aging hook yet, so this event is emitted by the dispatcher once it
 * gains one; the recorder method that fills it already exists so wiring it later touches no metric
 * code.
 */
@Name("tuff.compute.AgingActivated")
@Label("Compute Priority Aging Activated")
@Description("Priority aging lifted a waiting compute task")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class AgingActivatedEvent extends Event {

    /**
     * Consumer of the aged task.
     */
    @Label("Consumer")
    public String consumerId;

    /**
     * Priority the task was submitted with.
     */
    @Label("Submitted Priority")
    public String fromPriority;

    /**
     * Effective priority after the aging step.
     */
    @Label("Effective Priority")
    public String toPriority;

    /**
     * How long the task had been waiting.
     */
    @Label("Queue Wait")
    public long waitedNanos;
}
