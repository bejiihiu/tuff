package kz.bejiihiu.tuff.threads.internal.impl.registry;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.internal.impl.factory.ThreadFactoryProviderImpl;
import kz.bejiihiu.tuff.threads.internal.testing.FakePoolRegistry;
import kz.bejiihiu.tuff.threads.internal.testing.RecordingEventSink;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the unregistered thread report: it finds threads nobody accounts, and it leaves registered ones
 * alone.
 */
class UnregisteredThreadsTest {

    private static final PoolId POOL = PoolId.of("test-compute");
    private static final Thread.UncaughtExceptionHandler HANDLER = (thread, error) -> { };

    private final FakePoolRegistry pools = new FakePoolRegistry();
    private final RecordingEventSink sink = new RecordingEventSink();
    private final ThreadRegistryImpl registry = new ThreadRegistryImpl(this.sink);
    private final ThreadFactoryProviderImpl provider = new ThreadFactoryProviderImpl(this.pools, this.registry);
    private final UnregisteredThreads report = new UnregisteredThreads(this.registry, this.sink);

    @Test
    void findsThreadsThatNobodyAccounts() throws InterruptedException {
        final AtomicReference<Thread> stray = new AtomicReference<>();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch stop = new CountDownLatch(1);
        final Thread thread = Thread.ofPlatform().daemon(true).name("stray-worker-42").start(() -> {
            stray.set(Thread.currentThread());
            started.countDown();
            try {
                stop.await();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            final List<UnregisteredThreads.Entry> found = this.report.scan(
                candidate -> candidate != stray.get() && UnregisteredThreads.defaultIgnore().test(candidate)
            );

            final Optional<UnregisteredThreads.Entry> entry = found.stream()
                .filter(candidate -> candidate.threadId() == stray.get().threadId())
                .findFirst();
            assertTrue(entry.isPresent());
            assertEquals("stray-worker-42", entry.get().name());
            assertTrue(entry.get().daemon());
            assertFalse(entry.get().stackAtScan().isEmpty());
            assertTrue(this.sink.unregisteredNames().contains("stray-worker-42"));
        } finally {
            stop.countDown();
            thread.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void registeredThreadsAreNotReported() {
        this.pools.register(new PoolSpec(
            POOL, PoolKind.COMPUTE, "test", "unit test pool", 1, -1, CoverageLevel.L2, ResourceBudget.defaults()
        ));
        final Thread thread = this.provider.newThread(ThreadSpec.of(POOL, "known-worker", HANDLER), () -> { });
        try {
            final List<UnregisteredThreads.Entry> found = this.report.scan(
                candidate -> candidate != thread && UnregisteredThreads.defaultIgnore().test(candidate)
            );

            assertTrue(found.stream().noneMatch(candidate -> candidate.threadId() == thread.threadId()));
        } finally {
            this.registry.unregister(thread);
        }
    }

    @Test
    void defaultIgnoreSkipsKnownOutsideThreads() throws InterruptedException {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch stop = new CountDownLatch(1);
        final Thread thread = Thread.ofPlatform().daemon(true).name("Reference Handler helper").start(() -> {
            started.countDown();
            try {
                stop.await();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            // evaluated from the test thread, otherwise the self check would do the work
            assertTrue(UnregisteredThreads.defaultIgnore().test(thread));
            assertFalse(UnregisteredThreads.defaultIgnore().test(new Thread(() -> { }, "stray-worker-7")));
        } finally {
            stop.countDown();
            thread.join(TimeUnit.SECONDS.toMillis(5));
        }
    }
}
