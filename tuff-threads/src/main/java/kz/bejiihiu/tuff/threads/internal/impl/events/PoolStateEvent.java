package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * One sampling pass of a single pool.
 *
 * <p>Emitted from the sampler thread, never from a worker, so the event describes a moment slightly in
 * the past rather than the caller's own state. Counts that the pool cannot report are {@code -1} here
 * too: a zero would read as "the queue is empty" instead of "this pool does not expose a queue".
 */
@Name("tuff.threads.PoolState")
@Label("Thread Pool State")
@Description("Periodic sample of one pool: thread counts, queue and saturation")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class PoolStateEvent extends Event {

    /**
     * Stable pool id.
     */
    @Label("Pool")
    public String poolId;

    /**
     * Pool kind, as declared in the registry.
     */
    @Label("Kind")
    public String poolKind;

    /**
     * Integration level, {@code L0} to {@code L4}.
     */
    @Label("Coverage")
    public String coverage;

    /**
     * Threads that currently exist in the pool.
     */
    @Label("Alive Threads")
    public int aliveThreads;

    /**
     * Threads the pool keeps alive regardless of load.
     */
    @Label("Core Threads")
    public int coreThreads;

    /**
     * Threads that are runnable, or {@code -1} when the pool cannot classify them.
     */
    @Label("Active Threads")
    public int activeThreads;

    /**
     * Threads that are waiting for work, or {@code -1} when the pool cannot classify them.
     */
    @Label("Idle Threads")
    public int idleThreads;

    /**
     * Tasks waiting in the pool queue, or {@code -1} when the depth is not exposed.
     */
    @Label("Queued Tasks")
    public long queuedTasks;

    /**
     * Maximum queue depth, or {@code -1} when unknown or unbounded.
     */
    @Label("Queue Capacity")
    public long queueCapacity;

    /**
     * Share of the usable cores consumed by the pool, or {@code -1} when unavailable.
     */
    @Label("CPU Load")
    public double cpuLoad;

    /**
     * Summary saturation level.
     */
    @Label("Saturation")
    public String saturation;

    /**
     * Comma separated degraded reasons, empty when the pool is fully manageable.
     */
    @Label("Degraded Reasons")
    public String degradedReasons;
}
