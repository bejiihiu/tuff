package kz.bejiihiu.tuff.compute.internal.worker.impl;

import kz.bejiihiu.tuff.compute.ComputeSettings;

/**
 * Turns the configured worker sizing into concrete thread counts.
 *
 * <p>Fixed sizing is taken literally. Automatic sizing has to leave room for the threads that are
 * already part of a Folia server: one or more region tick threads (Folia uses {@code cores / 2}
 * divided by four, but never less than one), the global region thread, GC workers and JIT compiler
 * threads. The formula used here is
 *
 * <pre>
 * tickThreads = cores &lt;= 8 ? 1 : cores / 8        // Folia TickRegions, rounded down
 * reserve     = max(2, ceil(cores / 8))           // GC, JIT, network, watchdog
 * target      = cores - tickThreads - 1 - reserve
 * </pre>
 *
 * <p>The extra {@code 1} is the global region thread. The result is clamped into the configured
 * {@code [minThreads, maxThreads]} window and never exceeds the processor count, because a CPU bound
 * pool that oversubscribes the machine only adds context switches and jitter to the tick threads.
 */
final class WorkerSizing {

    private WorkerSizing() {
    }

    /**
     * Returns the initial and desired worker count for the supplied settings.
     *
     * @param workers             worker sizing from the configuration
     * @param availableProcessors processors reported by the JVM, at least {@code 1}
     * @return the concrete worker count, between {@code 1} and {@link #maxWorkers}
     */
    static int targetWorkers(final ComputeSettings.Workers workers, final int availableProcessors) {
        final int maximum = maxWorkers(workers, availableProcessors);
        final int target;
        if (workers.automaticThreadCount()) {
            target = automaticTarget(availableProcessors);
        } else {
            target = workers.threads();
        }
        return Math.clamp(target, workers.minThreads(), maximum);
    }

    /**
     * Returns the hard ceiling for the pool.
     *
     * <p>An explicit {@code maxThreads} is taken as written: the administrator may deliberately
     * oversubscribe the machine. When it is {@code -1} the contract resolves it to the processor
     * count, which is the right ceiling for a CPU bound pool.
     *
     * @param workers              worker sizing from the configuration
     * @param availableProcessors  processors reported by the JVM, at least {@code 1}
     * @return the maximum worker count, never below {@code 1} or the configured minimum
     */
    static int maxWorkers(final ComputeSettings.Workers workers, final int availableProcessors) {
        return Math.max(1, Math.max(workers.minThreads(), workers.resolveMaxThreads(availableProcessors)));
    }

    /**
     * Returns the raw automatic target before clamping to the configured window.
     *
     * @param cores processors reported by the JVM, at least {@code 1}
     * @return the number of workers the server can afford, possibly below {@code 1}
     */
    static int automaticTarget(final int cores) {
        final int tickThreads = cores <= 8 ? 1 : cores / 8;
        final int reserve = Math.max(2, (cores + 7) / 8);
        return cores - tickThreads - 1 - reserve;
    }
}
