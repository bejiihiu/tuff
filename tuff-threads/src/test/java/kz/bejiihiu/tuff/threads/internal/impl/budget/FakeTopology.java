package kz.bejiihiu.tuff.threads.internal.impl.budget;

import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;

/**
 * Topology double with a processor count that a test can change.
 *
 * <p>Changing it after a plan exists is exactly the "the platform reported a different topology" case:
 * the controller must keep the first ledger, so the test can prove the plan is not recomputed.
 */
public final class FakeTopology implements TopologyView {

    private final AtomicInteger processors;
    private final int numaNodes;

    /**
     * Creates a single node topology.
     *
     * @param processors processor count, at least one
     * @throws IllegalArgumentException if {@code processors} is below one
     */
    public FakeTopology(final int processors) {
        this(processors, 1);
    }

    /**
     * Creates a topology.
     *
     * @param processors processor count, at least one
     * @param numaNodes  NUMA node count, at least one
     * @throws IllegalArgumentException if a count is below one
     */
    public FakeTopology(final int processors, final int numaNodes) {
        if (processors < 1) {
            throw new IllegalArgumentException("processors must be at least 1, got " + processors);
        }
        if (numaNodes < 1) {
            throw new IllegalArgumentException("numaNodes must be at least 1, got " + numaNodes);
        }
        this.processors = new AtomicInteger(processors);
        this.numaNodes = numaNodes;
    }

    @Override
    public int availableProcessors() {
        return this.processors.get();
    }

    @Override
    public int totalCores() {
        return this.processors.get();
    }

    @Override
    public boolean numaAvailable() {
        return this.numaNodes > 1;
    }

    @Override
    public int numaNodeCount() {
        return this.numaNodes;
    }

    /**
     * Changes the reported processor count.
     *
     * @param count new count, at least one
     * @throws IllegalArgumentException if {@code count} is below one
     */
    public void setProcessors(final int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least 1, got " + count);
        }
        this.processors.set(count);
    }
}
