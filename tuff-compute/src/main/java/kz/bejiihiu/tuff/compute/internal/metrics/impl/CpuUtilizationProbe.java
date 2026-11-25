package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.OptionalDouble;

/**
 * Honest CPU utilisation estimate for the scheduler snapshot.
 *
 * <p>The only figure this class reports is the JVM process CPU load from
 * {@link OperatingSystemMXBean}. Two things follow from that and are worth
 * stating instead of hiding:
 * <ul>
 *     <li>it is process wide, not pool wide — the server's tick threads, GC and JIT are included, so
 *     a busy scheduler on an idle process shows up as a fraction of the process load, not as a
 *     fraction of the workers;</li>
 *     <li>it is an operating system estimate with an implementation defined averaging window. On
 *     Linux it is derived from {@code /proc}, on Windows from the process times, and some runtimes
 *     report a negative value to mean "unavailable".</li>
 * </ul>
 *
 * <p>Whenever the bean is missing, the method is unsupported or the runtime answers with a negative
 * number, {@link #sample()} returns {@link OptionalDouble#empty()} and the scheduler snapshot reports
 * {@link Double#NaN}. No fallback number is invented: a made up utilisation is worse than none,
 * because operators would act on it.
 */
public final class CpuUtilizationProbe {

    private static final OperatingSystemMXBean OS_BEAN = probeBean();

    private CpuUtilizationProbe() {
    }

    /**
     * Checks whether the runtime exposes a process CPU load estimate.
     *
     * @return {@code true} if {@link #sample()} can return a value
     */
    public static boolean supported() {
        return OS_BEAN != null;
    }

    /**
     * Samples the process CPU load.
     *
     * @return a value in {@code [0.0, 1.0]}, or an empty optional when the estimate is unavailable
     */
    public static OptionalDouble sample() {
        if (OS_BEAN == null) {
            return OptionalDouble.empty();
        }
        final double load;
        try {
            load = OS_BEAN.getProcessCpuLoad();
        } catch (RuntimeException | LinkageError _) {
            // a runtime that exposes the interface but not the implementation: treat as unavailable
            return OptionalDouble.empty();
        }
        if (!Double.isFinite(load) || load < 0.0d) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(Math.min(1.0d, load));
    }

    /**
     * Resolves the extended operating system bean once.
     *
     * @return the bean, or {@code null} when this runtime does not provide it
     */
    private static OperatingSystemMXBean probeBean() {
        try {
            if (ManagementFactory.getOperatingSystemMXBean() instanceof final OperatingSystemMXBean bean) {
                return bean;
            }
        } catch (RuntimeException | LinkageError _) {
            // the platform bean is optional; the scheduler keeps working without it
        }
        return null;
    }
}
