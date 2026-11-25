package kz.bejiihiu.tuff.compute.internal.worker.impl;

import kz.bejiihiu.tuff.compute.ComputeSettings;

/**
 * Turns the configured worker sizing into concrete thread counts.
 *
 * <p>The processor count passed in is a <b>budget</b>: the number of processors the compute engine
 * may actually use, already reduced by the server's region tick threads, chunk workers, IO threads
 * and a small reserve. The platform side owns that budget; the engine never subtracts from the raw
 * core count itself, because doing both would leave the pool with far fewer threads than the
 * administrator asked for.
 *
 * <p>Sizing is therefore a pure delegation to {@link ComputeSettings.Workers#resolveThreadCount(int)}
 * and {@link ComputeSettings.Workers#resolveMaxThreads(int)}: one formula, one place to change it.
 */
final class WorkerSizing {

    private WorkerSizing() {
    }

    /**
     * Returns the initial and desired worker count for the supplied settings.
     *
     * @param workers worker sizing from the configuration
     * @param budget  processors available to the engine, at least {@code 1}
     * @return the concrete worker count, never below {@code 1} and never above {@link #maxWorkers}
     */
    static int targetWorkers(final ComputeSettings.Workers workers, final int budget) {
        return workers.resolveThreadCount(budget);
    }

    /**
     * Returns the hard ceiling for the pool.
     *
     * @param workers worker sizing from the configuration
     * @param budget  processors available to the engine, at least {@code 1}
     * @return the maximum worker count, never below {@code 1}
     */
    static int maxWorkers(final ComputeSettings.Workers workers, final int budget) {
        return workers.resolveMaxThreads(budget);
    }
}
