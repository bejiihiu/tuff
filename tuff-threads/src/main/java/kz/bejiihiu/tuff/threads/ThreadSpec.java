package kz.bejiihiu.tuff.threads;

import java.util.Objects;

/**
 * Description of one thread the manager should create.
 *
 * <p>A spec is a value: the factory may create any number of threads from it, each one getting the
 * same pool tag, daemon status, priority, handler and group, plus a unique suffix in the name. It
 * never wraps or replaces the thread class, because Folia checks the concrete type of its tick
 * threads; the factory always lets the caller supply the body that the thread runs.
 *
 * <p>Priority is carried because some pools ask for it, not because it is a scheduling tool on the
 * platforms Tuff targets. On Linux a non privileged JVM ignores {@link Thread#setPriority(int)}
 * unless {@code -XX:ThreadPriorityPolicy=1} is set and the process has {@code CAP_SYS_NICE}; virtual
 * threads ignore it always. Latency is bought with deadlines, budgets and queue discipline, never with
 * nice values.
 *
 * @param pool                     pool the thread belongs to, used for accounting and ownership checks
 * @param name                     base name of the thread, used verbatim plus a numeric suffix
 * @param daemon                   whether the thread is a daemon thread
 * @param priority                 Java thread priority between {@link Thread#MIN_PRIORITY} and
 *                                 {@link Thread#MAX_PRIORITY}
 * @param uncaughtExceptionHandler handler installed on the created thread
 * @param threadGroup              group the thread is created in
 */
public record ThreadSpec(
    PoolId pool,
    String name,
    boolean daemon,
    int priority,
    Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
    ThreadGroup threadGroup
) {

    /**
     * Validates a thread description.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if the name is blank or the priority is out of range
     */
    public ThreadSpec {
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uncaughtExceptionHandler, "uncaughtExceptionHandler");
        Objects.requireNonNull(threadGroup, "threadGroup");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException(
                "priority must be between " + Thread.MIN_PRIORITY + " and " + Thread.MAX_PRIORITY
                    + ", got " + priority
            );
        }
    }

    /**
     * Creates a description for a daemon thread in the current thread group.
     *
     * <p>Most internal pools are daemons: they must not keep the JVM alive after the server stopped.
     * Region tick threads, watchdogs and shutdown helpers are not, so those integrations build the
     * spec explicitly or use {@link #withDaemon(boolean)}.
     *
     * @param pool    pool the thread belongs to
     * @param name    base name of the thread
     * @param handler handler installed on the created thread
     * @return the description
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if the name is blank
     */
    public static ThreadSpec of(
        final PoolId pool,
        final String name,
        final Thread.UncaughtExceptionHandler handler
    ) {
        return new ThreadSpec(
            pool,
            name,
            true,
            Thread.NORM_PRIORITY,
            handler,
            Thread.currentThread().getThreadGroup()
        );
    }

    /**
     * Returns a copy with another base name.
     *
     * @param newName base name of the thread
     * @return the copy
     * @throws NullPointerException     if {@code newName} is {@code null}
     * @throws IllegalArgumentException if {@code newName} is blank
     */
    public ThreadSpec withName(final String newName) {
        return new ThreadSpec(
            this.pool,
            newName,
            this.daemon,
            this.priority,
            this.uncaughtExceptionHandler,
            this.threadGroup
        );
    }

    /**
     * Returns a copy with another daemon status.
     *
     * @param newDaemon whether the thread is a daemon thread
     * @return the copy
     */
    public ThreadSpec withDaemon(final boolean newDaemon) {
        return new ThreadSpec(
            this.pool,
            this.name,
            newDaemon,
            this.priority,
            this.uncaughtExceptionHandler,
            this.threadGroup
        );
    }

    /**
     * Returns a copy with another priority.
     *
     * @param newPriority Java thread priority, between {@link Thread#MIN_PRIORITY} and
     *                    {@link Thread#MAX_PRIORITY}
     * @return the copy
     * @throws IllegalArgumentException if the priority is out of range
     */
    public ThreadSpec withPriority(final int newPriority) {
        return new ThreadSpec(
            this.pool,
            this.name,
            this.daemon,
            newPriority,
            this.uncaughtExceptionHandler,
            this.threadGroup
        );
    }

    /**
     * Returns a copy with another uncaught exception handler.
     *
     * @param handler handler installed on the created thread
     * @return the copy
     * @throws NullPointerException if {@code handler} is {@code null}
     */
    public ThreadSpec withUncaughtExceptionHandler(final Thread.UncaughtExceptionHandler handler) {
        return new ThreadSpec(this.pool, this.name, this.daemon, this.priority, handler, this.threadGroup);
    }

    /**
     * Returns a copy in another thread group.
     *
     * @param group group the thread is created in
     * @return the copy
     * @throws NullPointerException if {@code group} is {@code null}
     */
    public ThreadSpec withThreadGroup(final ThreadGroup group) {
        return new ThreadSpec(this.pool, this.name, this.daemon, this.priority, this.uncaughtExceptionHandler, group);
    }
}
