package kz.bejiihiu.tuff.threads.internal.impl.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.internal.impl.factory.ThreadFactoryProviderImpl;
import kz.bejiihiu.tuff.threads.internal.testing.FakePoolRegistry;
import kz.bejiihiu.tuff.threads.internal.testing.RecordingEventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the bookkeeping contract: one pool per thread, reference counted registration, monotonic
 * counters, and no leftover entries once a thread is gone.
 */
class ThreadRegistryImplTest {

    private static final PoolId POOL = PoolId.of("test-compute");
    private static final Thread.UncaughtExceptionHandler HANDLER = (thread, error) -> { };

    private final FakePoolRegistry pools = new FakePoolRegistry();
    private final RecordingEventSink sink = new RecordingEventSink();
    private final ThreadRegistryImpl registry = new ThreadRegistryImpl(this.sink);
    private final ThreadFactoryProviderImpl provider = new ThreadFactoryProviderImpl(this.pools, this.registry);

    @BeforeEach
    void registerPool() {
        this.pools.register(new PoolSpec(
            POOL, PoolKind.COMPUTE, "test", "unit test pool", 1, -1, CoverageLevel.L2, ResourceBudget.defaults()
        ));
    }

    @Test
    void createdAndTerminatedAreCountedPerPool() throws InterruptedException {
        final ThreadSpec spec = ThreadSpec.of(POOL, "counted", HANDLER);

        for (int i = 0; i < 5; i++) {
            final Thread thread = this.provider.newThread(spec, () -> { });
            thread.start();
            thread.join();
        }

        assertEquals(5L, this.registry.createdTotal(POOL));
        assertEquals(5L, this.registry.terminatedTotal(POOL));
        assertEquals(0, this.registry.aliveThreads(POOL));
        assertTrue(this.registry.snapshot().isEmpty());
        assertEquals(5, this.sink.created());
        assertEquals(5, this.sink.terminated());
    }

    @Test
    void registrationIsReferenceCounted() {
        final Thread thread = new Thread(() -> { }, "refcount-thread");

        this.registry.register(thread, POOL);
        this.registry.register(thread, POOL);
        assertEquals(1, this.registry.aliveThreads(POOL));
        assertEquals(1L, this.registry.createdTotal(POOL));

        this.registry.unregister(thread);
        assertEquals(1, this.registry.aliveThreads(POOL));
        assertEquals(0L, this.registry.terminatedTotal(POOL));

        this.registry.unregister(thread);
        assertEquals(0, this.registry.aliveThreads(POOL));
        assertEquals(1L, this.registry.terminatedTotal(POOL));
    }

    @Test
    void unregisterIsIdempotentForUnknownThreads() {
        final Thread thread = new Thread(() -> { }, "never-registered");

        this.registry.unregister(thread);
        this.registry.unregister(thread);

        assertEquals(0L, this.registry.terminatedTotal(POOL));
        assertTrue(this.registry.poolOf(thread).isEmpty());
    }

    @Test
    void aThreadCannotChangePools() {
        final Thread thread = new Thread(() -> { }, "switcher");
        this.registry.register(thread, POOL);

        assertThrows(IllegalStateException.class, () -> this.registry.register(thread, PoolId.of("elsewhere")));
        assertEquals(Optional.of(POOL), this.registry.poolOf(thread));
    }

    @Test
    void snapshotCapturesPoolStateAndIdentity() {
        final Thread thread = Thread.ofPlatform().daemon(true).name("snapshot-thread").unstarted(() -> { });
        this.registry.register(thread, POOL);

        final List<ThreadSnapshot> snapshots = this.registry.snapshot();

        assertEquals(1, snapshots.size());
        final ThreadSnapshot snapshot = snapshots.getFirst();
        assertEquals(thread.threadId(), snapshot.threadId());
        assertEquals("snapshot-thread", snapshot.name());
        assertEquals(POOL, snapshot.pool());
        assertEquals(Thread.State.NEW, snapshot.state());
        assertTrue(snapshot.daemon());
        assertTrue(snapshot.cpuTimeNanos() >= -1L);

        this.registry.unregister(thread);
    }

    @Test
    void terminatedThreadsDoNotLeakIntoTheRegistry() throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            final Thread thread = new Thread(() -> { }, "leak-check-" + i);
            // registered directly, without the factory's exit hook: the registry still has to drop it
            this.registry.register(thread, POOL);
            thread.start();
            thread.join();
        }

        assertTrue(this.registry.threads(POOL).isEmpty());
        assertEquals(0, this.registry.aliveThreads(POOL));
        assertTrue(this.registry.snapshot().isEmpty());
        assertTrue(this.registry.threads(POOL).isEmpty());
        assertEquals(50L, this.registry.createdTotal(POOL));
        assertEquals(50L, this.registry.terminatedTotal(POOL));
    }

    @Test
    void registrationRefusedAfterStopButCleanupStillWorks() {
        final Thread thread = new Thread(() -> { }, "after-stop");
        this.registry.register(thread, POOL);

        this.registry.markStopped();

        assertTrue(this.registry.isStopped());
        assertThrows(IllegalStateException.class, () -> this.registry.register(new Thread(() -> { }), POOL));

        this.registry.unregister(thread);
        assertTrue(this.registry.poolOf(thread).isEmpty());
        assertEquals(1L, this.registry.terminatedTotal(POOL));
    }

    @Test
    void concurrentCreationAndTerminationStayConsistent() throws InterruptedException {
        final int workers = 8;
        final int perWorker = 25;
        final CountDownLatch gate = new CountDownLatch(1);
        final AtomicInteger failures = new AtomicInteger();
        final List<Thread> creators = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            creators.add(Thread.ofPlatform().name("creator-" + i).start(() -> {
                try {
                    gate.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int j = 0; j < perWorker; j++) {
                    try {
                        final Thread worker = this.provider.newThread(
                            ThreadSpec.of(POOL, "stress-worker", HANDLER), () -> { }
                        );
                        worker.start();
                        worker.join();
                    } catch (RuntimeException | InterruptedException _) {
                        failures.incrementAndGet();
                        return;
                    }
                }
            }));
        }

        gate.countDown();
        for (final Thread creator : creators) {
            creator.join();
        }

        assertEquals(0, failures.get());
        assertEquals(workers * perWorker, this.registry.createdTotal(POOL));
        assertEquals(workers * perWorker, this.registry.terminatedTotal(POOL));
        assertEquals(0, this.registry.aliveThreads(POOL));
        assertTrue(this.registry.snapshot().isEmpty());
        assertEquals(0, this.sink.created() - this.sink.terminated());
    }

    @Test
    void unknownPoolReadsAreEmpty() {
        final PoolId unknown = PoolId.of("no-such-pool");

        assertTrue(this.registry.threads(unknown).isEmpty());
        assertEquals(0, this.registry.aliveThreads(unknown));
        assertEquals(0L, this.registry.createdTotal(unknown));
        assertEquals(0L, this.registry.terminatedTotal(unknown));
        assertTrue(this.registry.poolOf(new Thread(() -> { })).isEmpty());
    }
}
