package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.impl.events.MetricsEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;

/**
 * Hand rolled fakes for the metrics tests.
 *
 * <p>They exist because the interesting inputs are "a thread that is waiting", "a runtime without CPU
 * accounting" and "a probe that reports a queue", and none of those are cheap or deterministic with a
 * real server. Everything the sampler reads is a seam, so the fakes are small.
 */
final class MetricsFixtures {

    private MetricsFixtures() {
    }

    /**
     * Clock a test moves by hand.
     */
    static final class FakeClock implements MonotonicClock {

        private long nanos;

        FakeClock() {
            this(0L);
        }

        FakeClock(final long startNanos) {
            this.nanos = startNanos;
        }

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
     * Counters a test sets, including the unsupported CPU case.
     */
    static final class FakeCounters implements ThreadCounters {

        private final Map<Long, Long> cpu = new HashMap<>();
        private boolean supported = true;
        private long started;
        private long live;
        private double processLoad = 0.25d;

        void cpu(final long threadId, final long nanos) {
            this.cpu.put(threadId, nanos);
        }

        void unsupported() {
            this.supported = false;
        }

        void threads(final long startedTotal, final long liveCount) {
            this.started = startedTotal;
            this.live = liveCount;
        }

        void processLoad(final double load) {
            this.processLoad = load;
        }

        @Override
        public boolean cpuTimeSupported() {
            return this.supported;
        }

        @Override
        public long cpuTimeNanos(final long threadId) {
            if (!this.supported) {
                return -1L;
            }
            return this.cpu.getOrDefault(threadId, -1L);
        }

        @Override
        public long startedTotal() {
            return this.started;
        }

        @Override
        public long liveThreads() {
            return this.live;
        }

        @Override
        public double processCpuLoad() {
            return this.supported ? this.processLoad : -1.0d;
        }
    }

    /**
     * Topology with a fixed processor count.
     */
    static final class FakeTopology implements TopologyView {

        private final int processors;

        FakeTopology(final int processors) {
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
     * Registry whose snapshot is whatever the test declared.
     */
    static final class FakeRegistry implements ThreadRegistry {

        private final Map<PoolId, List<ThreadSnapshot>> byPool = new LinkedHashMap<>();

        void put(final PoolId pool, final ThreadSnapshot... threads) {
            this.byPool.put(pool, List.of(threads));
        }

        @Override
        public void register(final Thread thread, final PoolId pool) {
            throw new UnsupportedOperationException("read only fixture");
        }

        @Override
        public void unregister(final Thread thread) {
            // a thread may leave, the declared snapshot does not follow a real thread
        }

        @Override
        public Optional<PoolId> poolOf(final Thread thread) {
            return Optional.empty();
        }

        @Override
        public List<Thread> threads(final PoolId pool) {
            return List.of();
        }

        @Override
        public int aliveThreads(final PoolId pool) {
            return this.byPool.getOrDefault(pool, List.of()).size();
        }

        @Override
        public long createdTotal(final PoolId pool) {
            return 0L;
        }

        @Override
        public long terminatedTotal(final PoolId pool) {
            return 0L;
        }

        @Override
        public List<ThreadSnapshot> snapshot() {
            final List<ThreadSnapshot> all = new ArrayList<>();
            for (final List<ThreadSnapshot> threads : this.byPool.values()) {
                all.addAll(threads);
            }
            all.sort(Comparator.comparingLong(ThreadSnapshot::threadId));
            return List.copyOf(all);
        }
    }

    /**
     * Registry storage with a fixed set of specs.
     */
    static final class FakePools implements ExecutionPoolRegistry {

        private final Map<PoolId, PoolSpec> specs = new TreeMap<>();

        FakePools(final PoolSpec... initial) {
            for (final PoolSpec spec : initial) {
                this.specs.put(spec.id(), spec);
            }
        }

        @Override
        public PoolSpec register(final PoolSpec spec) {
            this.specs.put(spec.id(), spec);
            return spec;
        }

        @Override
        public boolean unregister(final PoolId id) {
            return this.specs.remove(id) != null;
        }

        @Override
        public Optional<PoolSpec> spec(final PoolId id) {
            return Optional.ofNullable(this.specs.get(id));
        }

        @Override
        public boolean contains(final PoolId id) {
            return this.specs.containsKey(id);
        }

        @Override
        public Set<PoolId> ids() {
            return Set.copyOf(this.specs.keySet());
        }

        @Override
        public List<PoolSpec> specs() {
            return List.copyOf(this.specs.values());
        }

        @Override
        public List<PoolSpec> specs(final PoolKind kind) {
            return this.specs.values().stream().filter(spec -> spec.kind() == kind).toList();
        }
    }

    /**
     * Sink that remembers everything and can blow up on purpose.
     */
    static final class RecordingSink implements MetricsEventSink {

        final List<PoolSnapshot> poolStates = new ArrayList<>();
        final List<SaturationReport> saturations = new ArrayList<>();
        final List<String> budgetChanges = new ArrayList<>();
        final List<ThreadSnapshot> created = new ArrayList<>();
        final List<ThreadSnapshot> terminated = new ArrayList<>();
        final List<String> unregistered = new ArrayList<>();
        private boolean explode;

        void explode() {
            this.explode = true;
        }

        @Override
        public void threadCreated(final ThreadSnapshot thread) {
            this.fail();
            this.created.add(thread);
        }

        @Override
        public void threadTerminated(final ThreadSnapshot thread) {
            this.fail();
            this.terminated.add(thread);
        }

        @Override
        public void poolState(final PoolSnapshot pool) {
            this.fail();
            this.poolStates.add(pool);
        }

        @Override
        public void saturation(final SaturationReport report) {
            this.fail();
            this.saturations.add(report);
        }

        @Override
        public void leaseTimedOut(final LeaseRequest request) {
            this.fail();
        }

        @Override
        public void unregisteredThread(final long threadId, final String name) {
            this.fail();
            this.unregistered.add(threadId + ":" + name);
        }

        @Override
        public void budgetChanged(final PoolId pool, final ResourceBudget previous, final ResourceBudget current) {
            this.fail();
            this.budgetChanges.add(pool + ":" + previous.cores() + "->" + current.cores());
        }

        private void fail() {
            if (this.explode) {
                throw new IllegalStateException("sink is broken on purpose");
            }
        }
    }

    /**
     * Controller that only answers the ledger and saturation, for wiring the sampler.
     */
    static final class FakeLeaseController implements ThreadResourceController {

        private final Map<PoolId, SaturationReport> reports = new HashMap<>();
        private final Map<PoolId, List<DegradedReason>> reasons = new HashMap<>();
        private final int usable;
        private SheddingReport shedding = SheddingReport.none();

        FakeLeaseController() {
            this(1);
        }

        FakeLeaseController(final int usable) {
            this.usable = usable;
        }

        void report(final SaturationReport report) {
            this.reports.put(report.pool(), report);
        }

        void reasons(final PoolId pool, final DegradedReason... values) {
            this.reasons.put(pool, List.of(values));
        }

        void shedding(final SheddingReport report) {
            this.shedding = report;
        }

        @Override
        public int usableCores() {
            return this.usable;
        }

        @Override
        public int criticalReserve() {
            return this.shedding.criticalReserveCores();
        }

        @Override
        public SheddingReport sheddingReport() {
            return this.shedding;
        }

        @Override
        public List<DegradedReason> degradedReasons(final PoolId pool) {
            return this.reasons.getOrDefault(pool, List.of());
        }

        @Override
        public String budgetLedger() {
            return "cores " + this.usable + ": ceiling " + this.usable
                + " = region 0 + chunk 0 + io 0 + reserve 0 + usable " + this.usable;
        }

        @Override
        public ResourceBudget totalBudget() {
            return ResourceBudget.defaults();
        }

        @Override
        public Optional<ResourceBudget> budget(final PoolId pool) {
            return Optional.empty();
        }

        @Override
        public Lease acquire(final LeaseRequest request) {
            throw new UnsupportedOperationException("read only fixture");
        }

        @Override
        public void release(final Lease lease) {
            throw new UnsupportedOperationException("read only fixture");
        }

        @Override
        public SaturationReport saturation(final PoolId pool) {
            return this.reports.getOrDefault(pool, SaturationReport.unknown(pool));
        }
    }

    /**
     * Probe with fixed counters and an optional thread list.
     */
    static final class FakeProbe implements PoolProbe {
        private final PoolId pool;
        private final PoolObservation observation;

        FakeProbe(final PoolId pool, final PoolObservation observation) {
            this.pool = pool;
            this.observation = observation;
        }

        @Override
        public PoolId pool() {
            return this.pool;
        }

        @Override
        public PoolObservation observe() {
            return this.observation;
        }
    }

    /**
     * Observation with fixed values.
     */
    static final class FakeObservation implements PoolObservation {

        private final int alive;
        private final int core;
        private final int active;
        private final int idle;
        private final long queued;
        private final long capacity;
        private final List<Thread> threads;

        FakeObservation(
            final int alive,
            final int core,
            final int active,
            final int idle,
            final long queued,
            final long capacity,
            final List<Thread> threads
        ) {
            this.alive = alive;
            this.core = core;
            this.active = active;
            this.idle = idle;
            this.queued = queued;
            this.capacity = capacity;
            this.threads = threads;
        }

        @Override
        public int aliveThreads() {
            return this.alive;
        }

        @Override
        public int coreThreads() {
            return this.core;
        }

        @Override
        public int activeThreads() {
            return this.active;
        }

        @Override
        public int idleThreads() {
            return this.idle;
        }

        @Override
        public long queuedTasks() {
            return this.queued;
        }

        @Override
        public long queueCapacity() {
            return this.capacity;
        }

        @Override
        public List<Thread> threads() {
            return this.threads;
        }
    }
}
