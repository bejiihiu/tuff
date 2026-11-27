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

    private KairosGate() {
    }

    /**
     * Records the operator's answer from {@code config/tuff.yml}.
     *
     * <p>Called once per start, before the region scheduler starts. A value that could not be read
     * arrives here as {@code false}, which is the safe answer: no opt-in, no Kairos.
     *
     * @param kairosUnstable whether {@code kairos-unstable} is true
     */
    public static void arm(final boolean kairosUnstable) {
        armed = kairosUnstable;
    }

    /**
     * Forgets the opt-in. Used on shutdown and by tests so a stopped layer never leaves a stale yes.
     */
    public static void disarm() {
        armed = false;
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
