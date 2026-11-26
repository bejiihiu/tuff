package kz.bejiihiu.tuff.threads;

import java.util.Objects;

/**
 * Point in time observation of one thread.
 *
 * <p>A snapshot is a copy, not a live view: the thread may have changed state, exited or been replaced
 * right after the snapshot was taken. Ids are the JVM thread id, which is stable for the lifetime of
 * the thread and is the only key resource metrics should use; names are for humans and may collide
 * between pools.
 *
 * @param threadId     JVM thread id, always positive
 * @param name         thread name at capture time
 * @param pool         pool the thread is accounted to
 * @param state        state at capture time
 * @param daemon       whether the thread is a daemon thread
 * @param priority     Java priority at capture time
 * @param cpuTimeNanos total CPU time consumed by the thread since it started, or {@code -1} when the
 *                     JVM cannot measure it
 */
public record ThreadSnapshot(
    long threadId,
    String name,
    PoolId pool,
    Thread.State state,
    boolean daemon,
    int priority,
    long cpuTimeNanos
) {

    /**
     * Validates a thread snapshot.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if the id is not positive or the CPU time is below {@code -1}
     */
    public ThreadSnapshot {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(state, "state");
        if (threadId <= 0L) {
            throw new IllegalArgumentException("threadId must be positive, got " + threadId);
        }
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (cpuTimeNanos < -1L) {
            throw new IllegalArgumentException("cpuTimeNanos must be -1 or non negative, got " + cpuTimeNanos);
        }
    }

    /**
     * Checks whether the thread is currently not runnable and waiting for work or a lock.
     *
     * <p>This is the approximation the sampler uses for "idle": a thread that is waiting or timed
     * waiting is not consuming CPU right now. A blocked thread is not idle, it is contention, so it is
     * excluded on purpose.
     *
     * @return {@code true} for {@link Thread.State#WAITING} and {@link Thread.State#TIMED_WAITING}
     */
    public boolean idle() {
        return this.state == Thread.State.WAITING || this.state == Thread.State.TIMED_WAITING;
    }

    /**
     * Checks whether the thread is currently executing or ready to execute.
     *
     * @return {@code true} for {@link Thread.State#RUNNABLE}
     */
    public boolean active() {
        return this.state == Thread.State.RUNNABLE;
    }

    /**
     * Checks whether the JVM reported CPU time for this thread.
     *
     * @return {@code true} if {@link #cpuTimeNanos()} is not {@code -1}
     */
    public boolean cpuTimeKnown() {
        return this.cpuTimeNanos >= 0L;
    }
}
