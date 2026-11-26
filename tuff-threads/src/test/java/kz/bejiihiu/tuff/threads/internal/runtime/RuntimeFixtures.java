package kz.bejiihiu.tuff.threads.internal.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.impl.events.MetricsEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;

/**
 * Small deterministic doubles for the runtime tests.
 *
 * <p>Time is a number a test moves, the topology is a fixed processor count and the sink records
 * instead of writing JFR. Everything else in the runtime is the real component, because the point of
 * these tests is the wiring, not another mock of the wiring.
 */
final class RuntimeFixtures {

    private RuntimeFixtures() {
    }

    /**
     * Clock a test advances by hand.
     */
    static final class FakeClock implements MonotonicClock {

        private long nanos;

        void advance(final Duration duration) {
            this.nanos += duration.toNanos();
        }

        @Override
        public long nanoTime() {
            return this.nanos;
        }

        @Override
        public Instant instant() {
            return Instant.EPOCH.plusNanos(this.nanos);
        }
    }

    /**
     * Topology with a fixed processor count.
     */
    static final class FixedTopology implements TopologyView {

        private final int processors;

        FixedTopology(final int processors) {
            this.processors = processors;
        }

        @Override
        public int availableProcessors() {
            return this.processors;
        }

        @Override
        public int totalCores() {
            return this.processors;
        }

        @Override
        public boolean numaAvailable() {
            return false;
        }

        @Override
        public int numaNodeCount() {
            return 1;
        }
    }

    /**
     * Sink that remembers what happened.
     */
    static final class RecordingSink implements MetricsEventSink {

        private final List<PoolSnapshot> poolStates = new ArrayList<>();
        private final List<ThreadSnapshot> created = new ArrayList<>();
        private final List<ThreadSnapshot> terminated = new ArrayList<>();

        @Override
        public void threadCreated(final ThreadSnapshot thread) {
            this.created.add(thread);
        }

        @Override
        public void threadTerminated(final ThreadSnapshot thread) {
            this.terminated.add(thread);
        }

        @Override
        public void poolState(final PoolSnapshot pool) {
            this.poolStates.add(pool);
        }

        @Override
        public void saturation(final SaturationReport report) {
            // not asserted here
        }

        @Override
        public void leaseTimedOut(final LeaseRequest request) {
            // not asserted here
        }

        @Override
        public void unregisteredThread(final long threadId, final String name) {
            // not asserted here
        }

        @Override
        public void budgetChanged(final PoolId pool, final ResourceBudget previous, final ResourceBudget current) {
            // not asserted here
        }

        List<ThreadSnapshot> created() {
            return List.copyOf(this.created);
        }

        List<ThreadSnapshot> terminated() {
            return List.copyOf(this.terminated);
        }

        List<PoolSnapshot> poolStates() {
            return List.copyOf(this.poolStates);
        }
    }
}
