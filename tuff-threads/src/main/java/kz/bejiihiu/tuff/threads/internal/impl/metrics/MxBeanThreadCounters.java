package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.jspecify.annotations.Nullable;

/**
 * {@link ThreadCounters} backed by {@code ThreadMXBean} and the platform OS bean.
 *
 * <p>Per thread CPU time is enabled once, best effort: Folia already enables it for the tick timer, and a
 * runtime that refuses the call is not an error, it just means the sampler reports {@code -1}. The
 * support flag is read once at construction and never toggled, because enabling accounting after start
 * would change the meaning of every stored baseline.
 *
 * <p>The process load is an operating system estimate with a platform specific averaging window. A
 * negative or non finite answer means "unavailable" and is reported as {@code -1}; inventing a zero would
 * hide the fact that the number is missing.
 */
public final class MxBeanThreadCounters implements ThreadCounters {

    private static final @Nullable OperatingSystemMXBean OS_BEAN = probeOsBean();

    private final ThreadMXBean threads;
    private final boolean cpuTimeSupported;

    /**
     * Creates counters for the current JVM.
     */
    public MxBeanThreadCounters() {
        this.threads = ManagementFactory.getThreadMXBean();
        this.cpuTimeSupported = switchOn(this.threads);
    }

    /**
     * Enables CPU accounting and reports whether it works.
     *
     * @param bean thread bean to configure
     * @return {@code true} when per thread CPU time can be read
     */
    private static boolean switchOn(final ThreadMXBean bean) {
        if (!bean.isThreadCpuTimeSupported()) {
            return false;
        }
        try {
            if (!bean.isThreadCpuTimeEnabled()) {
                bean.setThreadCpuTimeEnabled(true);
            }
        } catch (UnsupportedOperationException _) {
            // a runtime that forbids enabling it reports -1 forever, which is still honest
            return false;
        } catch (SecurityException _) {
            // same story: no permission, no CPU time, no crash
            return false;
        }
        return true;
    }

    private static @Nullable OperatingSystemMXBean probeOsBean() {
        try {
            if (ManagementFactory.getOperatingSystemMXBean() instanceof final OperatingSystemMXBean bean) {
                return bean;
            }
        } catch (RuntimeException | LinkageError _) {
            // the platform bean is optional; the snapshot reports -1 without it
        }
        return null;
    }

    @Override
    public boolean cpuTimeSupported() {
        return this.cpuTimeSupported;
    }

    @Override
    public long cpuTimeNanos(final long threadId) {
        if (!this.cpuTimeSupported) {
            return -1L;
        }
        try {
            return this.threads.getThreadCpuTime(threadId);
        } catch (RuntimeException _) {
            return -1L;
        }
    }

    @Override
    public long startedTotal() {
        return this.threads.getTotalStartedThreadCount();
    }

    @Override
    public long liveThreads() {
        return this.threads.getThreadCount();
    }

    @Override
    public double processCpuLoad() {
        if (OS_BEAN == null) {
            return -1.0d;
        }
        final double load;
        try {
            load = OS_BEAN.getProcessCpuLoad();
        } catch (RuntimeException | LinkageError _) {
            return -1.0d;
        }
        if (!Double.isFinite(load) || load < 0.0d) {
            return -1.0d;
        }
        return Math.min(1.0d, load);
    }
}
