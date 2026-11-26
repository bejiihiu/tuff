package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Objects;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop;
import kz.bejiihiu.tuff.compute.internal.worker.impl.DynamicWorkerPool;
import org.jspecify.annotations.Nullable;

/**
 * One platform worker iteration: claim and run.
 *
 * <p>The loop performs exactly one task attempt per call and returns {@code false} when the
 * dispatcher has nothing runnable, which is the signal the pool uses to apply keep-alive. The claim
 * uses the platform level mask, so LOW entries are never handed to a platform worker while the
 * virtual lane owns them.
 *
 * <p>The execution and terminal accounting live in {@link EngineTaskRunner}; the virtual lane uses
 * the same runner after its own claim.
 */
final class EngineWorkerLoop implements WorkerLoop {

    private final DispatcherImpl dispatcher;
    private final EngineTaskRunner runner;

    /**
     * Pool that owns this loop, bound after construction because the pool needs the loop first.
     * Used only for the busy worker gauge.
     */
    private volatile @Nullable DynamicWorkerPool pool;

    /**
     * Creates the loop.
     *
     * @param dispatcher dispatcher that owns the queue and the quotas
     * @param runner     shared task runner
     */
    EngineWorkerLoop(final DispatcherImpl dispatcher, final EngineTaskRunner runner) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.runner = Objects.requireNonNull(runner, "runner");
    }

    /**
     * Binds the pool so the loop can report which workers are actually executing a task.
     *
     * @param workerPool pool that owns this loop
     */
    void bindPool(final DynamicWorkerPool workerPool) {
        this.pool = Objects.requireNonNull(workerPool, "workerPool");
    }

    @Override
    public boolean executeNext(final int workerIndex) {
        final QueueEntry entry = this.dispatcher.next(workerIndex);
        if (entry == null) {
            return false;
        }
        final DynamicWorkerPool owner = this.pool;
        if (owner != null) {
            owner.taskStarted();
        }
        try {
            this.runner.run(entry, LaneAwareQueue.Lane.PLATFORM);
        } finally {
            if (owner != null) {
                owner.taskFinished();
            }
        }
        return true;
    }

    @Override
    public void workerStarted(final int workerIndex) {
        // nothing per worker yet; the hook stays here so the pool contract is implemented locally
    }

    @Override
    public void workerStopped(final int workerIndex, final @Nullable Throwable cause) {
        this.dispatcher.workerStopped(workerIndex);
    }
}
