package kz.bejiihiu.tuff.threads.kairos;

/**
 * The explicit opt-in the experimental region scheduler needs before it may start.
 *
 * <p>The two files that decide this are read in the opposite order to the one you would expect:
 * {@code config/paper-global.yml} selects the scheduler while the server configuration is loaded,
 * which is before {@code config/tuff.yml} is read in the {@code CraftServer} constructor. So the
 * scheduler object exists before the opt-in is known. This holder carries the flag across that
 * window: the thread bootstrap arms it from {@code thread-management.region-execution.kairos-unstable}
 * and {@link KairosScheduler#start()} reads it once the region scheduler actually starts.
 *
 * <p>Not armed means not opted in. There is deliberately no silent fall back to EDF: an operator who
 * wrote {@code scheduler: KAIROS} and forgot the flag gets a refused start with both keys in the
 * message, not a server that quietly runs a different scheduler than the file says.
 */
public final class KairosGate {

    private static final String REQUIREMENT =
        "thread-management.region-execution.kairos-unstable: true in config/tuff.yml";

    private static volatile boolean armed;
    private static volatile KairosTuning tuning = KairosTuning.fixed();
    private static volatile KairosKernel kernel = KairosKernel.defaults();
    private static volatile KairosServices services = KairosServices.none();

    private KairosGate() {
    }

    /**
     * Records the knobs that act on a single tick or permit.
     *
     * <p>They are published here by the configuration reader rather than carried with
     * {@link #arm(boolean, KairosTuning)} because the settings record they would belong to is owned
     * by the thread layer and shared with pools that know nothing about ticks. The gate is already
     * the value that crosses the window between reading {@code config/tuff.yml} and starting the
     * region scheduler, so the tick knobs travel with it and arrive at the same moment.
     *
     * <p>A value that could not be read arrives here as the default of its field, which is the stock
     * behaviour: no catch-up, no local backlogs, no lease warning.
     *
     * @param tickKernel the knobs to use once the scheduler starts, never {@code null}
     */
    public static void kernel(final KairosKernel tickKernel) {
        kernel = tickKernel;
    }

    /**
     * Returns the knobs that came with the last configuration read.
     *
     * @return the tick knobs to apply, never {@code null}; the stock behaviour when nothing was read
     */
    public static KairosKernel kernel() {
        return kernel;
    }

    /**
     * Records the operator's answer from {@code config/tuff.yml}, with a fixed fund.
     *
     * @param kairosUnstable whether {@code kairos-unstable} is true
     */
    public static void arm(final boolean kairosUnstable) {
        arm(kairosUnstable, KairosTuning.fixed());
    }

    /**
     * Records the operator's answer and the fund knobs from {@code config/tuff.yml}.
     *
     * <p>Called once per start, before the region scheduler starts. A value that could not be read
     * arrives here as {@code false}, which is the safe answer: no opt-in, no Kairos.
     *
     * @param kairosUnstable whether {@code kairos-unstable} is true
     * @param fundTuning     the fund limits to use once the scheduler starts, never {@code null}
     */
    public static void arm(final boolean kairosUnstable, final KairosTuning fundTuning) {
        arm(kairosUnstable, fundTuning, KairosServices.none());
    }

    /**
     * Records the operator's answer, the fund knobs and the runtime seams from {@code config/tuff.yml}.
     *
     * <p>The services arrive after the thread layer is up: the budget port reads its controller and the
     * supervisor source reads its factory, neither of which exists while the configuration is being
     * parsed. Passing {@link KairosServices#none()} is the honest "no runtime" answer - the fund then
     * uses the core count rule and a plain daemon supervisor.
     *
     * @param kairosUnstable whether {@code kairos-unstable} is true
     * @param fundTuning     the fund limits to use once the scheduler starts, never {@code null}
     * @param runtime        budget and supervisor sources, never {@code null}
     */
    public static void arm(
        final boolean kairosUnstable,
        final KairosTuning fundTuning,
        final KairosServices runtime
    ) {
        tuning = fundTuning;
        services = runtime;
        armed = kairosUnstable;
    }

    /**
     * Records the runtime seams of a thread layer that just started.
     *
     * <p>Called separately from {@link #arm(boolean, KairosTuning)} because the layer is built after the
     * opt-in is read. A layer that failed to start leaves the previous answer in place; {@code arm}
     * resets it to {@link KairosServices#none()} on the next start.
     *
     * @param runtime budget and supervisor sources, never {@code null}
     */
    public static void services(final KairosServices runtime) {
        services = runtime;
    }

    /**
     * Returns the runtime seams of the last armed answer.
     *
     * @return the services to use, never {@code null}
     */
    public static KairosServices services() {
        return services;
    }

    /**
     * Forgets the opt-in and the fund knobs. Used on shutdown and by tests so a stopped layer never
     * leaves a stale yes.
     */
    public static void disarm() {
        armed = false;
        tuning = KairosTuning.fixed();
        kernel = KairosKernel.defaults();
        services = KairosServices.none();
    }

    /**
     * Returns the fund knobs that came with the last armed answer.
     *
     * @return the tuning to apply, never {@code null}; the fixed fund when nothing was armed
     */
    public static KairosTuning tuning() {
        return tuning;
    }

    /**
     * Returns the last armed answer.
     *
     * @return {@code true} when the operator opted in
     */
    public static boolean armed() {
        return armed;
    }

    /**
     * Refuses to continue unless the operator opted in.
     *
     * @throws IllegalStateException if the flag is off or was never read
     */
    public static void requireOptIn() {
        if (!armed) {
            throw new IllegalStateException(
                "threaded-regions.scheduler is KAIROS in config/paper-global.yml, but the experimental "
                    + "scheduler was not opted in. Set " + REQUIREMENT + " and restart. "
                    + "Kairos does not fall back to EDF on its own."
            );
        }
    }
}
