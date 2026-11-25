package kz.bejiihiu.tuff.compute;

/**
 * Priority of a compute task.
 *
 * <p>A higher level is preferred when the scheduler picks the next task out of a non empty queue,
 * but it is a hint rather than a guarantee: consumer quotas, weighted fair scheduling and priority
 * aging all make sure that lower levels keep making progress while the pool is busy. A single
 * consumer cannot starve everybody else by submitting only {@link #CRITICAL} work.
 *
 * <p>The enum order matches the level order, highest first, so {@code values()} can be used for
 * sorting as well.
 */
public enum ComputePriority {

    /**
     * Work with a hard latency requirement, for example the answer a player is actively waiting for.
     * Use sparingly: an abundance of critical work degrades the whole scheduler into FIFO.
     */
    CRITICAL(3),

    /**
     * Urgent game logic that should not wait behind background work, for example a block update that
     * gates further region progress.
     */
    HIGH(2),

    /**
     * Ordinary compute work. This is the priority used when a caller does not specify one.
     */
    NORMAL(1),

    /**
     * Background work that may wait, for example speculative chunk prefetching or cache warming.
     */
    LOW(0);

    private final int level;

    ComputePriority(final int level) {
        this.level = level;
    }

    /**
     * Returns the numeric level of this priority.
     *
     * @return {@code 3} for {@link #CRITICAL} down to {@code 0} for {@link #LOW}
     */
    public int level() {
        return this.level;
    }

    /**
     * Checks whether this priority is strictly more urgent than the given one.
     *
     * @param other priority to compare against
     * @return {@code true} if this priority has a higher level than {@code other}
     */
    public boolean outranks(final ComputePriority other) {
        return this.level > other.level;
    }

    /**
     * Resolves a numeric level back to a priority.
     *
     * @param level level as returned by {@link #level()}, must be between {@code 0} and {@code 3}
     * @return the priority with the given level
     * @throws IllegalArgumentException if the level is outside the supported range
     */
    public static ComputePriority byLevel(final int level) {
        return switch (level) {
            case 3 -> CRITICAL;
            case 2 -> HIGH;
            case 1 -> NORMAL;
            case 0 -> LOW;
            default -> throw new IllegalArgumentException("unknown priority level: " + level);
        };
    }

    /**
     * Returns the priority used by submission overloads that do not take one explicitly.
     *
     * @return {@link #NORMAL}
     */
    public static ComputePriority defaultPriority() {
        return NORMAL;
    }
}
