package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop;
import org.jspecify.annotations.Nullable;

/**
 * Worker loop double. Models the engine contract: it takes an entry from the real queue, checks the
 * cooperative cancellation flag before starting it, catches task throwables and returns {@code false}
 * when the queue is empty so the pool applies keep-alive.
 */
final class TestWorkerLoop implements WorkerLoop {

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger cancelledBeforeStart = new AtomicInteger();
    final AtomicInteger workerStarts = new AtomicInteger();
    final AtomicInteger workerStops = new AtomicInteger();
    final AtomicInteger concurrent = new AtomicInteger();
    final AtomicInteger maxConcurrent = new AtomicInteger();
    final Set<Long> threadIds = ConcurrentHashMap.newKeySet();
    final Set<Integer> workerIndexes = ConcurrentHashMap.newKeySet();
    final AtomicInteger escapedFailuresRemaining = new AtomicInteger();
    final AtomicInteger steals = new AtomicInteger();
    private final TestTaskQueue queue;
    private final boolean workStealing;
    private volatile DynamicWorkerPool pool;

    TestWorkerLoop(final TestTaskQueue queue, final boolean workStealing) {
        this.queue = queue;
        this.workStealing = workStealing;
    }

    /**
     * Binds the pool so this loop reports real busy workers instead of inferred permits.
     *
     * @param workerPool pool that owns this loop
     */
    void bindPool(final DynamicWorkerPool workerPool) {
        this.pool = workerPool;
    }

    @Override
    public void workerStarted(final int workerIndex) {
        this.workerStarts.incrementAndGet();
        this.workerIndexes.add(workerIndex);
    }

    @Override
    public void workerStopped(final int workerIndex, final @Nullable Throwable cause) {
        this.workerStops.incrementAndGet();
    }

    @Override
    public boolean executeNext(final int workerIndex) {
        if (this.escapedFailuresRemaining.getAndUpdate(value -> value > 0 ? value - 1 : 0) > 0) {
            throw new IllegalStateException("synthetic loop failure");
        }
        QueueEntry entry = null;
        if (this.workStealing) {
            entry = this.queue.steal(workerIndex);
            if (entry != null) {
                this.steals.incrementAndGet();
            }
        }
        if (entry == null) {
            entry = this.queue.poll(workerIndex);
        }
        if (entry == null) {
            return false;
        }
        final TestTask task = (TestTask) entry.task();
        if (task.cancelled.get() || entry.handle().isCancellationRequested()) {
            this.cancelledBeforeStart.incrementAndGet();
            task.done.countDown();
            return true;
        }
        this.threadIds.add(Thread.currentThread().threadId());
        final int now = this.concurrent.incrementAndGet();
        this.maxConcurrent.accumulateAndGet(now, Math::max);
        this.executed.incrementAndGet();
        final DynamicWorkerPool owner = this.pool;
        if (owner != null) {
            owner.taskStarted();
        }
        try {
            task.handle.markRunning();
            entry.execute();
            task.handle.markCompleted();
        } catch (Throwable thrown) {
            task.failure = thrown;
            task.handle.markFailed(thrown);
        } finally {
            if (owner != null) {
                owner.taskFinished();
            }
            this.concurrent.decrementAndGet();
        }
        return true;
    }
}
