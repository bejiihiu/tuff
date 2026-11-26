package kz.bejiihiu.tuff.compute.internal.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factories for the worker pool.
 *
 * <p>Thread names are part of the operational contract: they show up in thread dumps, in watchdog
 * reports and in JFR recordings, so they follow one scheme: {@code <prefix>-<number>} with a
 * per-factory counter starting at one.
 */
public final class ThreadFactories {

    private ThreadFactories() {
    }

    /**
     * Creates a factory for platform threads.
     *
     * @param namePrefix prefix for thread names, must not be blank
     * @param daemon     whether created threads are daemons. Production pools pass {@code true}: a
     *                   worker stuck in user code must not keep the JVM alive once the server has
     *                   started its own shutdown sequence. Daemon threads still appear in a thread
     *                   dump, so this flag does not hide a stuck worker from diagnostics
     * @return the factory
     */
    public static ThreadFactory platform(final String namePrefix, final boolean daemon) {
        Preconditions.requireNonBlank(namePrefix, "namePrefix");
        final AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            final Thread thread = new Thread(runnable, namePrefix + "-" + counter.incrementAndGet());
            thread.setDaemon(daemon);
            return thread;
        };
    }

    /**
     * Creates a factory whose thread carries exactly {@code name}.
     *
     * <p>For the one-off service threads (janitor, shutdown, lane pump): their names are already
     * unique and code that looks for them (thread dumps, the shutdown report) expects the exact
     * string, so no counter is appended. Use one instance per thread, otherwise the names collide.
     *
     * @param name   exact thread name, must not be blank
     * @param daemon whether the created thread is a daemon
     * @return the factory
     */
    public static ThreadFactory named(final String name, final boolean daemon) {
        Preconditions.requireNonBlank(name, "name");
        return runnable -> {
            final Thread thread = new Thread(runnable, name);
            thread.setDaemon(daemon);
            return thread;
        };
    }

    /**
     * Creates a factory for virtual threads.
     *
     * <p>Virtual threads are a poor fit for CPU bound compute because they do not add parallelism,
     * they only make blocking cheaper. The compute engine uses this factory for its bounded virtual
     * lane, which carries LOW and spilled NORMAL work, and never for platform workers.
     *
     * @param namePrefix prefix for thread names, must not be blank
     * @return the factory
     */
    public static ThreadFactory virtual(final String namePrefix) {
        Preconditions.requireNonBlank(namePrefix, "namePrefix");
        return Thread.ofVirtual().name(namePrefix + "-", 1L).factory();
    }
}
