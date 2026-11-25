package kz.bejiihiu.tuff.compute.internal.adaptive.impl;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * Emitted once per adaptive evaluation that was actually allowed to run.
 *
 * <p>The periodic {@code WorkerState} event shows the limit in force; this event explains the
 * transition: the samples the controller saw, the limits before and after, and the reason it held or
 * moved. Without the reason a recording only shows numbers changing, which is not actionable.
 */
@Name("tuff.compute.AdaptiveDecision")
@Label("Compute Adaptive Concurrency Decision")
@Description("The adaptive controller evaluated its window and picked a concurrency limit")
@Category({"Tuff", "Compute"})
@StackTrace(false)
public final class AdaptiveDecisionEvent extends Event {

    /**
     * Limit before the decision.
     */
    @Label("Current Limit")
    public int currentLimit;

    /**
     * Limit after the decision; equal to the current one when the controller held.
     */
    @Label("Desired Limit")
    public int desiredLimit;

    /**
     * Configured lower bound.
     */
    @Label("Minimum Limit")
    public int minLimit;

    /**
     * Configured upper bound.
     */
    @Label("Maximum Limit")
    public int maxLimit;

    /**
     * Task samples the window contained, after ring truncation.
     */
    @Label("Samples")
    public int samples;

    /**
     * Average queue wait of the window.
     */
    @Label("Average Queue Wait")
    public long averageQueueWaitNanos;

    /**
     * Average execution time of the window.
     */
    @Label("Average Execution")
    public long averageExecutionNanos;

    /**
     * 99th percentile queue wait of the window.
     */
    @Label("P99 Queue Wait")
    public long p99QueueWaitNanos;

    /**
     * Little's law utilisation of the current limit.
     */
    @Label("Estimated Utilization")
    public double utilization;

    /**
     * Name of the decision reason.
     */
    @Label("Reason")
    public String reason;
}
