package kz.bejiihiu.tuff.threads.soak;

import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.jspecify.annotations.Nullable;

/**
 * Fixtures for the scheduler soak tests: a tick that records every execution, a factory that can
 * break on demand, and one bounded polling helper.
 *
 * <p>Everything here observes the real scheduler through its public surface. The factory is the only
 * place where faults are injected, which is exactly where a real thread layer would fail: a refused
 * spawn, a blocked allocation, a supervisor that dies.
 */
final class SoakFixtures {

    private SoakFixtures() {
    }

    /**
     * Polls a condition until it holds.
     *
     * <p>Bounded by a deadline rather than by a fixed number of sleeps, so a slow machine only costs
     * time and never correctness. The failure message names the condition and the last observed value.
     *
     * @param what          description of the condition, used in the failure message
     * @param timeoutMillis maximum wait
     * @param condition     condition to wait for
     * @throws AssertionError if the condition is still false at the deadline
     */
    static void await(final String what, final long timeoutMillis, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("timed out after " + timeoutMillis + " ms waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    /**
     * A tick that counts its own executions and can reschedule itself a fixed number of times.
     *
     * <p>The deadline reader handed to the scheduler returns {@link #deadline()}, so a reschedule is
     * visible to the very next comparison, exactly like Folia's handle updating its start time.
     */
    static final class SoakTick extends SchedulableTick {

        private final String label;
        private final AtomicInteger runs = new AtomicInteger();
        private final AtomicInteger startsAfterHalt = new AtomicInteger();
        private final AtomicLong cpuNanos = new AtomicLong();
        private final CountDownLatch done;
        private final BooleanSupplier halted;
        private final @Nullable BooleanSupplier profiler;

        private volatile long deadline;
        private volatile int remainingReschedules;
        private final long runNanos;
        private final long periodNanos;

        /**
         * Creates a tick.
         *
         * @param label              name used in failure messages
         * @param deadline           initial deadline
         * @param reschedules        how many times the tick should reschedule itself
         * @param runNanos           nanoseconds of busy work per execution
         * @param periodNanos        delay between two executions of a rescheduling tick
         * @param done               latch counted down on every execution
         * @param halted             returns whether the scheduler was already halted
         * @param profiler           returns whether the profiler pass is on; may be {@code null}
         */
        SoakTick(
            final String label,
            final long deadline,
            final int reschedules,
            final long runNanos,
            final long periodNanos,
            final CountDownLatch done,
            final BooleanSupplier halted,
            final @Nullable BooleanSupplier profiler
        ) {
            this.label = label;
            this.deadline = deadline;
            this.remainingReschedules = reschedules;
            this.runNanos = runNanos;
            this.periodNanos = periodNanos;
            this.done = done;
            this.halted = halted;
            this.profiler = profiler;
            this.setScheduledStart(deadline);
        }

        /**
         * Returns the tick's label.
         *
         * @return the label given at construction
         */
        String label() {
            return this.label;
        }

        /**
         * Returns the current deadline.
         *
         * @return deadline in {@link System#nanoTime()} units
         */
        long deadline() {
            return this.deadline;
        }

        /**
         * Returns how many times the body ran.
         *
         * @return execution count
         */
        int runs() {
            return this.runs.get();
        }

        /**
         * Returns how many executions started after the scheduler was halted.
         *
         * @return the count, which must stay zero
         */
        int startsAfterHalt() {
            return this.startsAfterHalt.get();
        }

        /**
         * Returns the CPU time the tick body consumed, as observed from inside the tick thread.
         *
         * @return accumulated CPU nanoseconds
         */
        long cpuNanos() {
            return this.cpuNanos.get();
        }

        /**
         * Returns whether this tick is still scheduled to run again.
         *
         * @return {@code true} while the tick will reschedule
         */
        boolean pending() {
            return this.remainingReschedules > 0;
        }

        @Override
        public boolean runTick() {
            if (this.halted.getAsBoolean()) {
                this.startsAfterHalt.incrementAndGet();
            }
            final long cpuBefore = this.profiler != null && this.profiler.getAsBoolean()
                ? currentThreadCpuNanos()
                : 0L;
            this.runs.incrementAndGet();
            this.burn(this.runNanos);
            if (this.profiler != null && this.profiler.getAsBoolean()) {
                final long cpuAfter = currentThreadCpuNanos();
                this.cpuNanos.addAndGet(Math.max(0L, cpuAfter - cpuBefore));
            }
            if (this.remainingReschedules > 0) {
                --this.remainingReschedules;
                this.deadline = System.nanoTime() + this.periodNanos;
                this.setScheduledStart(this.deadline);
                this.done.countDown();
                return true;
            }
            this.done.countDown();
            return false;
        }

        @Override
        public boolean hasTasks() {
            return false;
        }

        @Override
        public boolean runTasks(final BooleanSupplier canContinue) {
            return false;
        }

        @Override
        public String toString() {
            return "SoakTick{" + this.label + '}';
        }

        private void burn(final long nanos) {
            if (nanos <= 0L) {
                return;
            }
            final long until = System.nanoTime() + nanos;
            while (System.nanoTime() - until < 0L) {
                Thread.onSpinWait();
            }
        }

        private static long currentThreadCpuNanos() {
            final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            if (!bean.isCurrentThreadCpuTimeSupported()) {
                return -1L;
            }
            return bean.getCurrentThreadCpuTime();
        }
    }

    /**
     * A thread factory that works normally and can then refuse or block one allocation.
     *
     * <p>{@code failAfter} models an {@link OutOfMemoryError} from the native thread allocation:
     * {@code newThread} throws. {@code blockAfter} models an allocator that stalls: the call announces
     * itself on {@link #entered()} and waits for {@link #release()}, which lets a test call
     * {@code halt()} while a spawn is genuinely in flight.
     */
    static final class FaultyThreadFactory implements ThreadFactory {

        private static final AtomicInteger GENERATIONS = new AtomicInteger();

        private final String prefix = "soak-thread-" + GENERATIONS.getAndIncrement() + '-';
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger ids = new AtomicInteger();
        private final List<Thread> created = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger starts = new AtomicInteger();
        private final int failAfter;
        private final int blockAfter;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private FaultyThreadFactory(final int failAfter, final int blockAfter) {
            this.failAfter = failAfter;
            this.blockAfter = blockAfter;
        }

        /**
         * Returns a factory that works for {@code allowed} threads and then throws.
         *
         * @param allowed number of successful allocations
         * @return the factory
         */
        static FaultyThreadFactory failingAfter(final int allowed) {
            return new FaultyThreadFactory(allowed, -1);
        }

        /**
         * Returns a factory that works for {@code allowed} threads and then blocks one allocation.
         *
         * @param allowed number of successful allocations before the blocked one
         * @return the factory
         */
        static FaultyThreadFactory blockingAfter(final int allowed) {
            return new FaultyThreadFactory(-1, allowed);
        }

        /**
         * Returns a factory that never fails.
         *
         * @return the factory
         */
        static FaultyThreadFactory normal() {
            return new FaultyThreadFactory(-1, -1);
        }

        @Override
        public Thread newThread(final Runnable run) {
            final int call = this.calls.getAndIncrement();
            if (this.failAfter >= 0 && call >= this.failAfter) {
                throw new OutOfMemoryError("no more native threads in this test");
            }
            if (this.blockAfter >= 0 && call == this.blockAfter) {
                this.entered.countDown();
                try {
                    if (!this.released.await(30L, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("the test never released the blocked allocation");
                    }
                } catch (final InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while blocking the allocation", failure);
                }
            }
            final Thread thread = new Thread(() -> {
                this.starts.incrementAndGet();
                run.run();
            }, this.prefix + this.ids.getAndIncrement());
            thread.setDaemon(true);
            this.created.add(thread);
            return thread;
        }

        /**
         * Returns every thread this factory built.
         *
         * @return a snapshot of the list
         */
        List<Thread> created() {
            return List.copyOf(this.created);
        }

        /**
         * Returns how many threads actually entered their body.
         *
         * @return the start count
         */
        int starts() {
            return this.starts.get();
        }

        /**
         * Returns whether any created thread is still alive.
         *
         * @return {@code true} while a thread leaks
         */
        boolean anyAlive() {
            return this.created.stream().anyMatch(Thread::isAlive);
        }

        /**
         * Waits for the blocked allocation to announce itself.
         *
         * @param timeoutMillis maximum wait
         * @return {@code true} when the allocation is blocked
         */
        boolean awaitEntered(final long timeoutMillis) {
            try {
                return this.entered.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        /**
         * Lets the blocked allocation continue.
         */
        void release() {
            this.released.countDown();
        }
    }
}
