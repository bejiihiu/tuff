package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Objects;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Assembly point of the compute engine.
 *
 * <p>This is the only supported way to build an {@link AsyncComputeScheduler}. The server bootstrap
 * reads {@code config/tuff.yml} into {@link ComputeSettings} and calls
 * {@link #create(ComputeSettings)}; nothing else is needed, and no component outside this package
 * has to know the queue, pool or metrics implementation.
 *
 * <p>The returned scheduler is in {@code NEW}. It owns no thread until {@link AsyncComputeScheduler#start()};
 * a scheduler created with {@link ComputeSettings#enabled()} false still starts and moves to
 * {@code RUNNING}, but refuses submissions with
 * {@link kz.bejiihiu.tuff.compute.ComputeRejectedException.Reason#SCHEDULER_DISABLED} and never creates
 * a worker.
 */
public final class ComputeSchedulers {

    private ComputeSchedulers() {
    }

    /**
     * Creates a stopped scheduler with the system clock.
     *
     * @param settings configuration of the scheduler
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    public static AsyncComputeScheduler create(final ComputeSettings settings) {
        Objects.requireNonNull(settings, "settings");
        return new AsyncComputeSchedulerImpl(new ComputeEngineImpl(settings, ComputeClock.system()));
    }

    /**
     * Creates a stopped scheduler with an explicit clock and processor count.
     *
     * <p>Runtimes that override the processor count, and deterministic tests, use this overload so
     * automatic worker sizing, aging, expiry and the adaptive window do not depend on the host.
     *
     * @param settings            configuration of the scheduler
     * @param clock               clock every component must use
     * @param availableProcessors processors available to the JVM, at least {@code 1}
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if {@code availableProcessors} is not positive
     */
    public static AsyncComputeScheduler create(
        final ComputeSettings settings,
        final ComputeClock clock,
        final int availableProcessors
    ) {
        return new AsyncComputeSchedulerImpl(new ComputeEngineImpl(settings, clock, availableProcessors));
    }
}
