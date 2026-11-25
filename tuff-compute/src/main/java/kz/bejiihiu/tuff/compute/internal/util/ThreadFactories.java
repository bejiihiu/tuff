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
     * @param daemon     whether created threads are daemons; server pools normally use
     *                   {@code false} so a stuck task is visible in a thread dump
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
     * Creates a factory for virtual threads.
     *
     * <p>Virtual threads are a poor fit for CPU bound compute because they do not add parallelism,
     * they only make blocking cheaper. The factory exists for experiments and for pools that mix
     * compute with blocking IO; the default scheduler configuration never uses it.
     *
     * @param namePrefix prefix for thread names, must not be blank
     * @return the factory
     */
    public static ThreadFactory virtual(final String namePrefix) {
        Preconditions.requireNonBlank(namePrefix, "namePrefix");
        return Thread.ofVirtual().name(namePrefix + "-", 1L).factory();
    }
}
