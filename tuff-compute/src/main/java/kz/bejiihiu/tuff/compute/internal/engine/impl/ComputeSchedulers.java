package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Objects;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeBudgetPort;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Assembly point of the compute engine.
 *
 * <p>This is the only supported way to build an {@link AsyncComputeScheduler}. The server bootstrap
 * reads {@code config/tuff.yml} into {@link ComputeSettings} and calls
 * {@link #create(ComputeSettings)}; nothing else is needed, and no component outside this package
 * has to know the queue, pool or metrics implementation.
 *
 * <p>An outer thread manager plugs in through two ports instead of a compile dependency: {@link
 * ComputeThreadFactoryPort} (where threads come from) and {@link ComputeBudgetPort} (where the
 * processor budget comes from). The port overloads default every argument that is not supplied to
 * the legacy behaviour, so a server without a manager keeps the exact same threads, names and
 * sizes.
 *
 * <p>The returned scheduler is in {@code NEW}. It owns no thread until {@link AsyncComputeScheduler#start()};
 * a scheduler created with {@link ComputeSettings#enabled()} false still starts and moves to
 * {@code RUNNING}, but refuses submissions with
 * {@link kz.bejiihiu.tuff.compute.ComputeRejectedException.Reason#SCHEDULER_DISABLED} and never creates
 * a worker.
 *
 * <p>Worker sizing uses a processor <b>budget</b>: the processors left after the platform reserved its
 * region/chunk/IO threads. The no-argument factory uses the JVM processor count as the initial
 * budget; a server that only learns the real budget after region startup should call
 * {@link AsyncComputeScheduler#applyProcessorBudget(int)} once it does.
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
        return create(settings, ComputeClock.system(), ComputeBudgetPort.legacy());
    }

    /**
     * Creates a stopped scheduler with an explicit clock and processor budget.
     *
     * <p>The third argument is a <b>budget</b>, not the raw core count: the number of processors the
     * engine may use after the platform reserved its region tick threads, chunk workers, IO threads
     * and a small headroom. The engine does not subtract those threads again, it only clamps the pool
     * into {@code minThreads}/{@code maxThreads}. Runtimes that only learn the budget after their
     * regions started create the scheduler with the JVM processor count and call
     * {@link AsyncComputeScheduler#applyProcessorBudget(int)} once the real budget is known.
     *
     * <p>Tests use this overload to make worker sizing and the adaptive window independent of the
     * host.
     *
     * @param settings        configuration of the scheduler
     * @param clock           clock every component must use
     * @param processorBudget processors available to the engine, at least {@code 1}
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException     if an argument is {@code null}
     * @throws IllegalArgumentException if {@code processorBudget} is not positive
     */
    public static AsyncComputeScheduler create(
        final ComputeSettings settings,
        final ComputeClock clock,
        final int processorBudget
    ) {
        return new AsyncComputeSchedulerImpl(new ComputeEngineImpl(settings, clock, processorBudget));
    }

    /**
     * Creates a stopped scheduler whose budget comes from a port, with legacy thread factories.
     *
     * <p>The port is asked exactly once, when the engine is built. It owns the reserve arithmetic, so
     * the engine must not subtract anything from the number again.
     *
     * @param settings configuration of the scheduler
     * @param clock    clock every component must use
     * @param budget   source of the initial processor budget
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException if an argument is {@code null}
     */
    public static AsyncComputeScheduler create(
        final ComputeSettings settings,
        final ComputeClock clock,
        final ComputeBudgetPort budget
    ) {
        return create(settings, clock, budget, ComputeThreadFactoryPort.legacy());
    }

    /**
     * Creates a stopped scheduler from both ports.
     *
     * <p>This is the manager integration point: threads come from {@code threads}, the initial
     * processor budget from {@code budget}. Passing the legacy ports gives a scheduler identical to
     * {@link #create(ComputeSettings, ComputeClock, int)} with the JVM processor count.
     *
     * @param settings configuration of the scheduler
     * @param clock    clock every component must use
     * @param budget   source of the initial processor budget
     * @param threads  source of every thread the engine creates
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException if an argument is {@code null}
     */
    public static AsyncComputeScheduler create(
        final ComputeSettings settings,
        final ComputeClock clock,
        final ComputeBudgetPort budget,
        final ComputeThreadFactoryPort threads
    ) {
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(clock, "clock");
        return new AsyncComputeSchedulerImpl(new ComputeEngineImpl(settings, clock, budget, threads));
    }

    /**
     * Creates a stopped scheduler with region relief configuration.
     *
     * @param settings configuration of the scheduler
     * @param clock    clock every component must use
     * @param budget   source of the initial processor budget
     * @param threads  source of every thread the engine creates
     * @param relief   region relief configuration
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException if an argument is {@code null}
     */
    public static AsyncComputeScheduler create(
        final ComputeSettings settings,
        final ComputeClock clock,
        final ComputeBudgetPort budget,
        final ComputeThreadFactoryPort threads,
        final RegionReliefSettings relief
    ) {
        return create(settings, clock, budget, threads, relief, false);
    }

    /**
     * Creates a stopped scheduler whose first worker spawn waits for the first processor budget.
     *
     * <p>Use this only when the real budget is not known yet. The pool is running and accepts work
     * from the start, but creates no worker until {@link AsyncComputeScheduler#applyProcessorBudget(int)}
     * is called; that call spawns the pool at the real size exactly once. An embedder that knows its
     * budget at construction must use the overloads without the flag.
     *
     * @param settings         configuration of the scheduler
     * @param clock            clock every component must use
     * @param budget           source of the initial processor budget
     * @param threads          source of every thread the engine creates
     * @param relief           region relief configuration
     * @param deferWorkerSpawn whether the first worker spawn waits for the first processor budget
     * @return a scheduler in {@link AsyncComputeScheduler.SchedulerState#NEW}
     * @throws NullPointerException if an argument is {@code null}
     */
    public static AsyncComputeScheduler create(
        final ComputeSettings settings,
        final ComputeClock clock,
        final ComputeBudgetPort budget,
        final ComputeThreadFactoryPort threads,
        final RegionReliefSettings relief,
        final boolean deferWorkerSpawn
    ) {
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(clock, "clock");
        final int processors = Objects.requireNonNull(budget, "budget").processors();
        return new AsyncComputeSchedulerImpl(new ComputeEngineImpl(
            settings,
            clock,
            processors,
            threads,
            relief,
            deferWorkerSpawn
        ));
    }
}
