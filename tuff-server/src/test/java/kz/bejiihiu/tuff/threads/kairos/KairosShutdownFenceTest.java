package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shutdown guarantee against a spawn that is still in flight.
 *
 * <p>The fund promises that every thread it ever created is dead when {@code join} returns. A spawn
 * that passed its halt check and then started a thread after the join had already walked the thread
 * list would break that promise silently; a save racing that thread is the one way this class can
 * corrupt a world. The test parks a spawn between registering its thread and starting it, halts the
 * fund, and checks that join keeps waiting for the thread instead of reporting an empty fund.
 */
@Timeout(120)
class KairosShutdownFenceTest {

    private final List<String> order = Collections.synchronizedList(new ArrayList<>());

    private @Nullable KairosScheduler scheduler;
    private @Nullable FencedFactory fenced;

    @AfterEach
    void stopEverything() {
        final FencedFactory factory = this.fenced;
        if (factory != null) {
            // never leave the fake start or body blocked: halt cannot interrupt either
            factory.releaseStart();
            factory.releaseBody();
        }
        if (this.scheduler != null) {
            this.scheduler.halt();
            if (!this.scheduler.join(10_000L)) {
                throw new AssertionError("the fund did not stop: live " + this.scheduler.liveThreads());
            }
            assertEquals(0, this.scheduler.liveThreads(), "live must be zero once every thread is dead");
            assertEquals(0, this.scheduler.getAliveThreads().length);
        }
        KairosGate.disarm();
    }

    @Test
    void joinWaitsForASpawnThatHasNotStartedItsThreadYet() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(true, 3, 60_000, 100, 20L));
        final FencedFactory factory = this.fenced = new FencedFactory(1);
        final KairosScheduler elastic = this.scheduler =
            new KairosScheduler(1, factory, tick -> ((FakeTick) tick).deadline());
        final long now = System.nanoTime();
        final CountDownLatch done = new CountDownLatch(3);
        for (int index = 0; index < 3; ++index) {
            elastic.schedule(new FakeTick("t" + index, now, this.order, done).runFor(TimeUnit.SECONDS.toNanos(5L)));
        }
        elastic.start();

        assertTrue(factory.awaitStartEntered(5_000L), "the supervisor must reach the spawn's thread start");
        elastic.halt();

        final AtomicBoolean joined = new AtomicBoolean();
        final Thread joiner = Thread.ofPlatform().daemon().name("fence-joiner").start(() -> {
            joined.set(elastic.join(10_000L));
        });

        // let the spawn finish: it starts its thread, which waits for the test before its runner loop
        factory.releaseStart();
        assertTrue(factory.awaitBodyEntered(5_000L), "the spawned thread must be alive and waiting");

        // a join that only looked at "is the thread alive" before the start would already have returned
        final long settleUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300L);
        while (System.nanoTime() - settleUntil < 0L && joiner.isAlive()) {
            Thread.onSpinWait();
        }
        assertTrue(joiner.isAlive(), "join must not report success while a spawned thread is alive");
        assertFalse(joined.get());

        factory.releaseBody();
        joiner.join(10_000L);
        assertFalse(joiner.isAlive(), "the joiner must finish once the thread exits");
        assertTrue(joined.get(), "join must return true after the last thread is dead");
        assertEquals(0, elastic.liveThreads(), "no runner may outlive the join");
        assertEquals(0, elastic.getAliveThreads().length);
        assertFalse(factory.anyAlive(), "no factory thread may survive the join");
        assertTrue(elastic.liveThreads() >= 0, "live can never go negative");
    }

    /**
     * A factory that blocks the spawned thread twice: before {@code Thread.start()} returns and then
     * inside the body, which is the window "registered but not started" the join contract has to close.
     */
    private static final class FencedFactory implements ThreadFactory {

        private final int plainThreads;
        private final AtomicInteger ids = new AtomicInteger();
        private final List<Thread> created = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch startEntered = new CountDownLatch(1);
        private final CountDownLatch allowStart = new CountDownLatch(1);
        private final CountDownLatch bodyEntered = new CountDownLatch(1);
        private final CountDownLatch allowBody = new CountDownLatch(1);

        private FencedFactory(final int plainThreads) {
            this.plainThreads = plainThreads;
        }

        @Override
        public Thread newThread(final Runnable run) {
            final int id = this.ids.getAndIncrement();
            final Thread thread;
            if (id < this.plainThreads) {
                thread = new Thread(run, "fence-floor-" + id);
            } else {
                final Runnable body = () -> {
                    this.bodyEntered.countDown();
                    await(this.allowBody);
                    run.run();
                };
                thread = new Thread(body, "fence-spawn-" + id) {
                    @Override
                    public void start() {
                        FencedFactory.this.startEntered.countDown();
                        await(FencedFactory.this.allowStart);
                        super.start();
                    }
                };
            }
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((ignored, failure) -> {
                // the test is about liveness, not about the death of a deliberately broken runner
            });
            this.created.add(thread);
            return thread;
        }

        private boolean awaitStartEntered(final long millis) throws InterruptedException {
            return this.startEntered.await(millis, TimeUnit.MILLISECONDS);
        }

        private boolean awaitBodyEntered(final long millis) throws InterruptedException {
            return this.bodyEntered.await(millis, TimeUnit.MILLISECONDS);
        }

        private void releaseStart() {
            this.allowStart.countDown();
        }

        private void releaseBody() {
            this.allowBody.countDown();
        }

        private boolean anyAlive() {
            synchronized (this.created) {
                return this.created.stream().anyMatch(Thread::isAlive);
            }
        }

        private static void await(final CountDownLatch latch) {
            try {
                if (!latch.await(30L, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the test never released the latch");
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
