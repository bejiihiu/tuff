package kz.bejiihiu.tuff.threads.internal.spi;

/**
 * Read only view of the processor topology the manager sizes against.
 *
 * <p>Two core counts exist on purpose and they disagree in containers. {@link #availableProcessors()}
 * is what the JVM may actually use: it respects cgroups, CPU quotas, job objects and
 * {@code -XX:ActiveProcessorCount}. {@link #totalCores()} is what the OS reports about the machine and
 * is kept for diagnostics only. Sizing uses the first one; the second one is shown next to it so an
 * operator can see why a container gets fewer threads than the host has cores.
 *
 * <p>A topology is read once at start and refreshed only when the platform reports a change. It never
 * attempts to change affinity, pinning or scheduling classes; those are explicitly out of scope.
 */
public interface TopologyView {

    /**
     * Returns the number of processors the JVM may use.
     *
     * <p>Container aware. This is the value every budget and default sizing formula starts from.
     *
     * @return available processors, at least one
     */
    int availableProcessors();

    /**
     * Returns the number of cores the operating system reports.
     *
     * <p>Informational. In a container this can be much larger than {@link #availableProcessors()};
     * never use it alone to size a pool.
     *
     * @return reported cores, at least one
     */
    int totalCores();

    /**
     * Checks whether a native NUMA topology is available.
     *
     * @return {@code true} only on platforms where nodes and distances can be read
     */
    boolean numaAvailable();

    /**
     * Returns the number of NUMA nodes, or one when NUMA is unavailable.
     *
     * @return node count, at least one
     */
    int numaNodeCount();
}
