package kz.bejiihiu.tuff.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;

/**
 * Fills a queue while one worker is held, so a benchmark can drain a deep backlog and time only the
 * drain.
 *
 * <p>Only the last future of every (consumer, priority) bucket is awaited: within one bucket the
 * queue is strictly ordered by submission sequence and a single worker executes it in that order, so
 * the last task of a bucket is also the last one to complete. That avoids attaching a dependent node
 * to all ten thousand futures, which would be measured instead of the dispatcher.
 */
final class DrainFixture {

    /**
     * Id of the consumer at the given index.
     *
     * @param index consumer index
     * @return stable consumer id
     */
    static String consumerId(final int index) {
        return "bench-" + index;
    }

    /**
     * Registers the consumers a drain benchmark submits under.
     *
     * @param scheduler scheduler to register on
     * @param consumers number of consumers
     */
    static void registerConsumers(final AsyncComputeScheduler scheduler, final int consumers) {
        for (int index = 0; index < consumers; index++) {
            scheduler.registerConsumer(ComputeConsumer.of(consumerId(index), ConsumerPolicy.defaults()));
        }
    }

    /**
     * Holds the only worker with a spinning blocker, queues the workload and returns a handle on the
     * release flag plus the completion flag of the drain.
     *
     * @param scheduler scheduler to fill
     * @param consumers number of consumers to spread the workload over
     * @param depth     number of queued tasks
     * @return the blocking handle, its release flag and the drain flag
     */
    static Filled fill(final AsyncComputeScheduler scheduler, final int consumers, final int depth) {
        final AtomicBoolean open = new AtomicBoolean();
        final ComputeTaskHandle<Boolean> blocker = scheduler.enqueue(new ComputeRequest<>(
            consumerId(0), ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                while (!open.get()) {
                    Thread.onSpinWait();
                }
                return Boolean.TRUE;
            }
        ));
        Fixtures.awaitState(blocker, ComputeTaskHandle.State.RUNNING);
        final CompletableFuture<?>[] lastPerBucket = new CompletableFuture<?>[consumers * 4];
        for (int index = 0; index < depth; index++) {
            final int consumer = index % consumers;
            final ComputePriority priority = ComputePriority.values()[index % 4];
            lastPerBucket[consumer * 4 + priority.ordinal()] = scheduler.submit(
                consumerId(consumer), ComputeTaskType.GENERAL, priority, Fixtures.NOOP);
        }
        final List<CompletableFuture<?>> last = new ArrayList<>(lastPerBucket.length);
        for (final CompletableFuture<?> future : lastPerBucket) {
            if (future != null) {
                last.add(future);
            }
        }
        final AtomicBoolean done = new AtomicBoolean();
        final CompletableFuture<?> drained =
            CompletableFuture.allOf(last.toArray(new CompletableFuture<?>[0]));
        drained.whenComplete((value, failure) -> done.set(true));
        return new Filled(blocker, open, done);
    }

    /**
     * State of a filled queue: the worker gate and a flag that flips when the drain is over.
     *
     * @param blocker blocking task that holds the worker
     * @param open    flag that releases the worker
     * @param done    set by the completion callback of the drain future
     */
    record Filled(ComputeTaskHandle<Boolean> blocker, AtomicBoolean open, AtomicBoolean done) {

        /**
         * Releases the blocked worker. The blocker spins instead of waiting on a latch, so this is a
         * plain flag write and no OS wake-up latency is measured.
         */
        void release() {
            this.open.set(true);
        }

        /**
         * Spins until the backlog is drained, then returns the flag for the blackhole.
         *
         * <p>The producer spins instead of parking on the future: a parked thread is woken by the OS
         * and that wake-up costs milliseconds on Windows, which would be measured instead of the
         * dispatcher.
         *
         * @return {@code true}
         */
        boolean awaitDrained() {
            while (!this.done.get()) {
                Thread.onSpinWait();
            }
            return this.done.get();
        }
    }
}
