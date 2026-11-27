package kz.bejiihiu.tuff.threads.internal.impl.factory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.internal.impl.registry.ThreadRegistryImpl;
import kz.bejiihiu.tuff.threads.internal.testing.FakePoolRegistry;
import kz.bejiihiu.tuff.threads.internal.testing.RecordingEventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the promises of the thread factory contract: spec applied verbatim, pool gate, wrapping without
 * touching the thread, and names that stay unique.
 */
class ThreadFactoryProviderImplTest {

    private static final PoolId POOL = PoolId.of("test-compute");
    private static final PoolId OTHER = PoolId.of("other-pool");
    private static final Thread.UncaughtExceptionHandler HANDLER = (thread, error) -> { };

    private final FakePoolRegistry pools = new FakePoolRegistry();
    private final RecordingEventSink sink = new RecordingEventSink();
    private final ThreadRegistryImpl registry = new ThreadRegistryImpl(this.sink);
    private final ThreadFactoryProviderImpl provider = new ThreadFactoryProviderImpl(this.pools, this.registry);

    @BeforeEach
    void registerComputePool() {
        this.pools.register(new PoolSpec(
            POOL, PoolKind.COMPUTE, "test", "unit test pool", 1, -1, CoverageLevel.L2, ResourceBudget.elastic(0.5d, 0)
        ));
    }

    @Test
    void newThreadAppliesTheSpec() throws InterruptedException {
        final ThreadSpec spec = ThreadSpec.of(POOL, "test-worker", HANDLER).withDaemon(true).withPriority(7);
        final AtomicReference<Optional<PoolId>> seen = new AtomicReference<>();

        final Thread thread = this.provider.newThread(
            spec, () -> seen.set(this.provider.poolOf(Thread.currentThread()))
        );

        assertEquals("test-worker-1", thread.getName());
        assertTrue(thread.isDaemon());
        assertEquals(7, thread.getPriority());
        assertSame(HANDLER, thread.getUncaughtExceptionHandler());
        assertEquals(Optional.of(POOL), this.provider.poolOf(thread));

        thread.start();
        thread.join();

        assertEquals(Optional.of(POOL), seen.get());
        assertTrue(this.provider.poolOf(thread).isEmpty());
    }

    @Test
    void namesAreUniquePerPoolAndBase() {
        final ThreadSpec spec = ThreadSpec.of(POOL, "dup", HANDLER);
        final Set<String> names = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            names.add(this.provider.newThread(spec, () -> { }).getName());
        }

        assertEquals(20, names.size());
        assertNotEquals("dup", names.iterator().next());
    }

    @Test
    void creationNeedsARegisteredPoolAtL2() {
        final ThreadSpec spec = ThreadSpec.of(OTHER, "test-worker", HANDLER);
        assertThrows(IllegalStateException.class, () -> this.provider.newThread(spec, () -> { }));

        this.pools.register(PoolSpec.observeOnly(OTHER, PoolKind.COMPUTE, "test", "observed only"));
        assertThrows(IllegalStateException.class, () -> this.provider.newThread(spec, () -> { }));
        assertThrows(IllegalStateException.class, () -> this.provider.factory(spec));
        assertThrows(IllegalStateException.class, () -> this.provider.wrap(spec, () -> { }));
    }

    @Test
    void l0IsRefusedByTheProviderItself() {
        final PoolId outside = PoolId.of("outside");
        this.pools.force(new PoolSpec(
            outside, PoolKind.SERVICE, "test", "outside the manager", 0, -1, CoverageLevel.L0, ResourceBudget.defaults()
        ));

        final ThreadSpec spec = ThreadSpec.of(outside, "outside-worker", HANDLER);
        assertThrows(IllegalStateException.class, () -> this.provider.newThread(spec, () -> { }));
    }

    @Test
    void virtualLaneCannotCreatePlatformThreads() {
        final PoolId virtual = PoolId.of("test-virtual");
        this.pools.register(new PoolSpec(
            virtual, PoolKind.VIRTUAL, "test", "virtual lane", 1, -1, CoverageLevel.L2, ResourceBudget.defaults()
        ));
        final ThreadSpec spec = ThreadSpec.of(virtual, "virtual-worker", HANDLER);

        assertThrows(IllegalStateException.class, () -> this.provider.newThread(spec, () -> { }));
        assertThrows(IllegalStateException.class, () -> this.provider.factory(spec));

        final AtomicReference<Optional<PoolId>> seen = new AtomicReference<>();
        final Runnable wrapped = this.provider.wrap(spec, () -> seen.set(this.provider.poolOf(Thread.currentThread())));
        final Thread carrier = Thread.ofVirtual().start(wrapped);
        try {
            carrier.join();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }

        assertEquals(Optional.of(virtual), seen.get());
    }

    @Test
    void factoryAppliesTheSpecToEveryThread() {
        final ThreadSpec spec = ThreadSpec.of(POOL, "factory-worker", HANDLER).withDaemon(false);

        final Thread first = this.provider.factory(spec).newThread(() -> { });
        final Thread second = this.provider.factory(spec).newThread(() -> { });

        assertEquals("factory-worker-1", first.getName());
        assertEquals("factory-worker-2", second.getName());
        assertFalse(first.isDaemon());
        assertFalse(second.isDaemon());
        assertEquals(Optional.of(POOL), this.registry.poolOf(first));
    }

    @Test
    void wrapKeepsClassGroupAndHandler() throws InterruptedException {
        final ThreadGroup group = new ThreadGroup("custom-region-group");
        final Thread.UncaughtExceptionHandler handler = (thread, error) -> { };
        final ThreadSpec spec = ThreadSpec.of(POOL, "region-worker", HANDLER);
        final AtomicReference<Optional<PoolId>> seen = new AtomicReference<>();
        final AtomicReference<ThreadGroup> seenGroup = new AtomicReference<>();
        final AtomicReference<Thread.UncaughtExceptionHandler> seenHandler = new AtomicReference<>();

        final Runnable wrapped = this.provider.wrap(spec, () -> {
            final Thread current = Thread.currentThread();
            seen.set(this.provider.poolOf(current));
            seenGroup.set(current.getThreadGroup());
            seenHandler.set(current.getUncaughtExceptionHandler());
            assertInstanceOf(CustomTickThread.class, current);
        });
        final Thread thread = new CustomTickThread(group, wrapped, "custom-tick-thread");
        thread.setUncaughtExceptionHandler(handler);
        assertSame(handler, thread.getUncaughtExceptionHandler());

        thread.start();
        thread.join();

        assertEquals(Optional.of(POOL), seen.get());
        assertSame(CustomTickThread.class, thread.getClass());
        assertSame(group, seenGroup.get());
        assertSame(handler, seenHandler.get());
        assertTrue(this.provider.poolOf(thread).isEmpty());
    }

    @Test
    void wrapFactoryKeepsTheDelegateThreadShape() throws InterruptedException {
        final ThreadGroup group = new ThreadGroup("delegate-group");
        final Thread.UncaughtExceptionHandler handler = (thread, error) -> { };
        final ThreadFactory delegate = body -> {
            final Thread thread = new CustomTickThread(group, body, "delegated-thread");
            thread.setUncaughtExceptionHandler(handler);
            return thread;
        };
        final ThreadSpec spec = ThreadSpec.of(POOL, "delegated", HANDLER);
        final AtomicReference<Optional<PoolId>> seen = new AtomicReference<>();

        final Thread thread = this.provider.wrapFactory(spec, delegate).newThread(
            () -> seen.set(this.provider.poolOf(Thread.currentThread()))
        );

        assertSame(CustomTickThread.class, thread.getClass());
        assertSame(group, thread.getThreadGroup());
        assertSame(handler, thread.getUncaughtExceptionHandler());
        // registration happens when the body runs, not when the factory is called
        assertTrue(this.provider.poolOf(thread).isEmpty());

        thread.start();
        thread.join();

        assertEquals(Optional.of(POOL), seen.get());
        assertTrue(this.provider.poolOf(thread).isEmpty());
        assertEquals(1, this.sink.created());
        assertEquals(1, this.sink.terminated());
    }

    @Test
    void wrapIsReferenceCountedPerThread() throws InterruptedException {
        final ThreadSpec spec = ThreadSpec.of(POOL, "nested", HANDLER);
        final AtomicReference<Integer> aliveInsideInner = new AtomicReference<>();

        final Runnable inner = this.provider.wrap(spec, () -> { });
        final Runnable outer = this.provider.wrap(spec, () -> {
            inner.run();
            aliveInsideInner.set(this.registry.aliveThreads(POOL));
        });
        final Thread thread = new Thread(outer, "nested-thread");

        thread.start();
        thread.join();

        assertEquals(1, aliveInsideInner.get().intValue());
        assertEquals(0, this.registry.aliveThreads(POOL));
        assertTrue(this.provider.poolOf(thread).isEmpty());
    }

    @Test
    void stoppedProviderRefusesNewWork() {
        final ThreadSpec spec = ThreadSpec.of(POOL, "after-stop", HANDLER);
        final Thread before = this.provider.newThread(spec, () -> { });
        final Runnable wrapped = this.provider.wrap(spec, () -> { });

        this.provider.markStopped();

        assertTrue(this.provider.isStopped());
        assertThrows(IllegalStateException.class, () -> this.provider.newThread(spec, () -> { }));
        assertThrows(IllegalStateException.class, () -> this.provider.factory(spec));
        assertThrows(IllegalStateException.class, () -> this.provider.wrap(spec, () -> { }));
        // a wrapper built before the stop still works, and the old thread is still accounted
        wrapped.run();
        assertEquals(Optional.of(POOL), this.provider.poolOf(before));
    }

    @Test
    void aWrapperBuiltBeforeTheRegistryStoppedStillRunsAfterIt() {
        final ThreadSpec spec = ThreadSpec.of(POOL, "late-body", HANDLER);
        final AtomicReference<Boolean> ran = new AtomicReference<>(false);
        final Runnable wrapped = this.provider.wrap(spec, () -> {
            ran.set(true);
            assertTrue(this.provider.poolOf(Thread.currentThread()).isEmpty(),
                "the stopped registry must not account a late body");
        });

        // the runtime order: the factory stops first, then the registry
        this.provider.markStopped();
        this.registry.markStopped();

        wrapped.run();

        assertEquals(Boolean.TRUE, ran.get(), "the body still runs; bookkeeping must not abort a tick");
        assertTrue(this.registry.isStopped());
    }

    @Test
    void poolOfUnknownThreadIsEmpty() {
        final Thread thread = new Thread(() -> { }, "not-ours");
        assertTrue(this.provider.poolOf(thread).isEmpty());
        assertThrows(NullPointerException.class, () -> this.provider.poolOf(null));
    }

    @Test
    void snapshotSeesCreatedThreadsImmediately() throws InterruptedException {
        final ThreadSpec spec = ThreadSpec.of(POOL, "snapshot-worker", HANDLER);
        final Thread thread = this.provider.newThread(spec, () -> { });

        assertEquals(1, this.registry.snapshot().size());
        assertEquals(1, this.registry.createdTotal(POOL));
        assertEquals(0, this.registry.terminatedTotal(POOL));

        thread.start();
        thread.join();
    }

    /**
     * Subclass whose identity the provider must leave alone, standing in for Folia's tick thread.
     */
    private static final class CustomTickThread extends Thread {

        private CustomTickThread(final ThreadGroup group, final Runnable body, final String name) {
            super(group, body, name);
        }
    }
}
