package kz.bejiihiu.tuff.threads.kairos;

/**
 * Knobs of the Kairos fund, read once when the scheduler starts.
 *
 * <p>The scheduler object is built while {@code paper-global.yml} is being read, which is before
 * {@code config/tuff.yml} exists as a parsed tree, so the operator's numbers cannot travel through
 * the constructor. {@link KairosGate} carries them across that window and {@link KairosScheduler#start()}
 * copies them into the fund. Changing any of them needs a restart, exactly like the rest of the
 * section.
 *
 * <p>{@code elastic == false} is the fixed fund of the first prototype: {@code min == max}, no
 * supervisor thread, no growth and no retire. It is the default, so a server that only flips the
 * scheduler stays on the behaviour that was already load tested.
 *
 * <p>The fields are: whether the fund is <em>elastic</em> at all; the hard ceiling of the fund; the
 * keep alive a runner must sit out before it may be retired; the spawn rate in new tick threads per
 * second; and the supervisor period between two passes.
 *
 * @param elastic                whether the fund may grow and shrink at all
 * @param maxThreads             hard ceiling of the fund, or {@code -1} for the core count rule
 * @param keepAliveMillis        how long a runner must sit idle before it may be retired, at least 1
 * @param spawnRatePerSecond     how many new tick threads the supervisor may start per second, at least 1
 * @param supervisorPeriodMillis pause between two supervisor passes, at least 1, at most 60000
 */
public record KairosTuning(
    boolean elastic,
    int maxThreads,
    int keepAliveMillis,
    int spawnRatePerSecond,
    long supervisorPeriodMillis
) {

    /**
     * Validates the knobs.
     *
     * @throws IllegalArgumentException if a limit is outside its range
     */
    public KairosTuning {
        if (maxThreads != -1 && maxThreads < 1) {
            throw new IllegalArgumentException("maxThreads must be -1 or at least 1, got " + maxThreads);
        }
        if (keepAliveMillis < 1) {
            throw new IllegalArgumentException("keepAliveMillis must be at least 1, got " + keepAliveMillis);
        }
        if (spawnRatePerSecond < 1) {
            throw new IllegalArgumentException(
                "spawnRatePerSecond must be at least 1, got " + spawnRatePerSecond
            );
        }
        if (supervisorPeriodMillis < 1L || supervisorPeriodMillis > 60_000L) {
            throw new IllegalArgumentException(
                "supervisorPeriodMillis must be in [1, 60000], got " + supervisorPeriodMillis
            );
        }
    }

    /**
     * Returns the fixed fund of the first prototype: no elasticity, no supervisor.
     *
     * @return the tuning that keeps {@code min == max}
     */
    public static KairosTuning fixed() {
        return new KairosTuning(false, -1, 30_000, 10, 1_000L);
    }

    /**
     * Returns the keep alive of an idle runner in nanoseconds.
     *
     * @return the configured keep alive, converted
     */
    public long keepAliveNanos() {
        return this.keepAliveMillis * 1_000_000L;
    }

    /**
     * Returns the pause between supervisor passes in nanoseconds.
     *
     * @return the configured period, converted
     */
    public long supervisorPeriodNanos() {
        return this.supervisorPeriodMillis * 1_000_000L;
    }
}
