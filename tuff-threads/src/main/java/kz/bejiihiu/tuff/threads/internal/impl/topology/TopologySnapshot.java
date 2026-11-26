package kz.bejiihiu.tuff.threads.internal.impl.topology;

import java.util.Objects;

/**
 * One immutable reading of the processor topology.
 *
 * <p>The record keeps both core counts and both container limits, because they answer different
 * questions. {@link #availableProcessors()} is the sizing input: it already respects a cgroup quota, a
 * cpuset, a job object and {@code -XX:ActiveProcessorCount}. {@link #possibleCpus()} is the kernel's
 * cpumask and is shown next to it for diagnostics only.
 *
 * <p>Anything the probe could not read is {@code -1}. That includes "no physical core information on
 * this platform" and "no cgroup because we are not in a container": both are unknown, not zero.
 *
 * @param availableProcessors processors the JVM may use, at least one
 * @param possibleCpus        CPUs the kernel reports as possible, or {@code -1} when unreadable
 * @param physicalCores       physical cores, or {@code -1} when the platform does not expose them
 * @param logicalCpus         logical CPUs, or {@code -1} when unreadable
 * @param siblingsPerCore     logical CPUs per physical core, or {@code -1} when unreadable
 * @param cgroupQuotaCores    CPU quota in core equivalents, or {@code -1} when there is none
 * @param cgroupCpusetCpus    size of the effective cpuset, or {@code -1} when there is none
 * @param numaNodeCount       NUMA nodes, at least one; one also means "not detected"
 * @param numaAvailable       whether more than one NUMA node was detected
 * @param source              where the reading came from, for logs and commands
 */
public record TopologySnapshot(
    int availableProcessors,
    int possibleCpus,
    int physicalCores,
    int logicalCpus,
    int siblingsPerCore,
    int cgroupQuotaCores,
    int cgroupCpusetCpus,
    int numaNodeCount,
    boolean numaAvailable,
    String source
) {

    /**
     * Validates a reading.
     *
     * @throws IllegalArgumentException if a count is out of range or the source is blank
     * @throws NullPointerException     if {@code source} is {@code null}
     */
    public TopologySnapshot {
        if (availableProcessors < 1) {
            throw new IllegalArgumentException(
                "availableProcessors must be at least 1, got " + availableProcessors
            );
        }
        requireUnknownable(possibleCpus, "possibleCpus");
        requireUnknownable(physicalCores, "physicalCores");
        requireUnknownable(logicalCpus, "logicalCpus");
        requireUnknownable(siblingsPerCore, "siblingsPerCore");
        requireUnknownable(cgroupQuotaCores, "cgroupQuotaCores");
        requireUnknownable(cgroupCpusetCpus, "cgroupCpusetCpus");
        if (numaNodeCount < 1) {
            throw new IllegalArgumentException("numaNodeCount must be at least 1, got " + numaNodeCount);
        }
        Objects.requireNonNull(source, "source");
        if (source.isBlank()) {
            throw new IllegalArgumentException("source must not be blank");
        }
    }

    /**
     * Creates the reading of a platform without any topology source.
     *
     * <p>This is what Windows, macOS and a failed probe look like: only the JVM count is known, NUMA is
     * a single node, everything else is unknown.
     *
     * @param availableProcessors processors the JVM may use, at least one
     * @return the minimal reading
     * @throws IllegalArgumentException if {@code availableProcessors} is below one
     */
    public static TopologySnapshot runtimeOnly(final int availableProcessors) {
        return new TopologySnapshot(availableProcessors, -1, -1, -1, -1, -1, -1, 1, false, "runtime");
    }

    /**
     * Checks whether the process sees fewer CPUs than the machine.
     *
     * <p>True inside a cpuset or a container with a CPU quota, and the reason a pool is smaller than a
     * host with the same hardware would suggest. Informational; it never changes a budget.
     *
     * @return {@code true} when a known possible or cpuset count exceeds {@link #availableProcessors()}
     */
    public boolean constrained() {
        return (this.possibleCpus > 0 && this.availableProcessors < this.possibleCpus)
            || (this.cgroupCpusetCpus > 0 && this.availableProcessors < this.cgroupCpusetCpus);
    }

    /**
     * Checks whether the probe believes it is inside a CPU quota.
     *
     * @return {@code true} when a quota was read and it is below {@link #possibleCpus()}
     */
    public boolean quotaLimited() {
        return this.cgroupQuotaCores > 0
            && (this.possibleCpus < 1 || this.cgroupQuotaCores < this.possibleCpus);
    }

    private static void requireUnknownable(final int value, final String name) {
        if (value < -1) {
            throw new IllegalArgumentException(name + " must be -1 or at least 0, got " + value);
        }
    }
}
