package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import kz.bejiihiu.tuff.compute.ComputeTask;
import org.jspecify.annotations.Nullable;

/**
 * A single unit of work used by the test loop.
 */
final class TestTask implements ComputeTask<Object> {

    private static final AtomicLong IDS = new AtomicLong();

    final TestTaskHandle handle;
    final AtomicBoolean cancelled = new AtomicBoolean();
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicBoolean started = new AtomicBoolean();
    volatile long threadId = -1L;
    volatile boolean interrupted;
    volatile @Nullable Throwable failure;
    private final Runnable body;

    TestTask(final Runnable body) {
        this(new TestTaskHandle(IDS.incrementAndGet(), "default"), body);
    }

    TestTask(final TestTaskHandle handle, final Runnable body) {
        this.handle = handle;
        this.body = body;
    }

    @Override
    public Object execute() {
        this.started.set(true);
        this.threadId = Thread.currentThread().threadId();
        this.interrupted = Thread.currentThread().isInterrupted();
        try {
            this.body.run();
            return null;
        } catch (Throwable thrown) {
            this.failure = thrown;
            throw thrown;
        } finally {
            this.done.countDown();
        }
    }
}
