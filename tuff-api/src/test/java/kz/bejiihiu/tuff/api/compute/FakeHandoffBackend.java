package kz.bejiihiu.tuff.api.compute;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.Plugin;
import org.jspecify.annotations.Nullable;

/**
 * Records what {@link ComputeHandoff} asked for instead of handing it to a scheduler. Nothing runs
 * until the test calls {@link #runActions()}, which is exactly how the region and global helpers
 * behave from a worker thread. A retired entity is simulated by {@link #retireEntities()}.
 */
final class FakeHandoffBackend implements HandoffBackend {

    /**
     * One recorded scheduling call.
     *
     * @param target           scheduler the delivery asked for
     * @param plugin           plugin passed to the scheduler
     * @param world            world of a region call, {@code null} for other targets
     * @param chunkX           chunk x of a region call, {@code 0} for other targets
     * @param chunkZ           chunk z of a region call, {@code 0} for other targets
     * @param entity           entity of an entity call, {@code null} for other targets
     * @param action           work the scheduler was supposed to run
     * @param retired          retired callback handed to the entity scheduler
     * @param schedulingThread thread that called the backend
     */
    record Call(
        Target target,
        Plugin plugin,
        @Nullable World world,
        int chunkX,
        int chunkZ,
        @Nullable Entity entity,
        Runnable action,
        @Nullable Runnable retired,
        Thread schedulingThread
    ) {
    }

    /**
     * Scheduler a call was made against.
     */
    enum Target {
        REGION,
        ENTITY,
        GLOBAL
    }

    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final List<Runnable> actions = new CopyOnWriteArrayList<>();
    private final AtomicInteger attempts = new AtomicInteger();
    private volatile @Nullable RuntimeException failure;
    private volatile boolean entityRetired;

    @Override
    public @Nullable ScheduledTask scheduleInRegion(
        final Plugin plugin,
        final World world,
        final int chunkX,
        final int chunkZ,
        final Runnable action
    ) {
        this.record(new Call(Target.REGION, plugin, world, chunkX, chunkZ, null, action, null, Thread.currentThread()));
        return new FakeScheduledTask(plugin);
    }

    @Override
    public @Nullable ScheduledTask scheduleOnEntity(
        final Plugin plugin,
        final Entity entity,
        final Runnable action,
        final Runnable retired
    ) {
        this.record(new Call(Target.ENTITY, plugin, null, 0, 0, entity, action, retired, Thread.currentThread()));
        return this.entityRetired ? null : new FakeScheduledTask(plugin);
    }

    @Override
    public @Nullable ScheduledTask scheduleInGlobal(final Plugin plugin, final Runnable action) {
        this.record(new Call(Target.GLOBAL, plugin, null, 0, 0, null, action, null, Thread.currentThread()));
        return new FakeScheduledTask(plugin);
    }

    /**
     * Makes every scheduling call throw, as a disabled plugin or a shutting down server does.
     *
     * @param throwable the failure the backend should throw
     */
    void failWith(final RuntimeException throwable) {
        this.failure = throwable;
    }

    /**
     * Makes the entity scheduler report the retired state by returning {@code null}.
     */
    void retireEntities() {
        this.entityRetired = true;
    }

    /**
     * Returns the recorded calls in the order they were made.
     *
     * @return an immutable snapshot of the calls
     */
    List<Call> calls() {
        return List.copyOf(this.calls);
    }

    /**
     * Returns the only recorded call.
     *
     * @return the single call, or {@code null} when there was not exactly one
     */
    @Nullable Call onlyCall() {
        return this.calls.size() == 1 ? this.calls.getFirst() : null;
    }

    /**
     * Runs every recorded action in the order it was scheduled, simulating the region tick.
     */
    void runActions() {
        for (final Runnable action : this.actions) {
            action.run();
        }
    }

    /**
     * Returns how many scheduling calls reached the backend, including the ones that threw.
     *
     * @return the number of attempts
     */
    int attempts() {
        return this.attempts.get();
    }

    private void record(final Call call) {
        this.attempts.incrementAndGet();
        final RuntimeException scheduledFailure = this.failure;
        if (scheduledFailure != null) {
            throw scheduledFailure;
        }
        this.calls.add(call);
        this.actions.add(call.action());
    }

    /**
     * Task returned by a non-retired schedule call. The handoff ignores it, so only the contract
     * members are implemented.
     *
     * @param plugin owning plugin the task reports
     */
    private record FakeScheduledTask(Plugin plugin) implements ScheduledTask {

        @Override
        public Plugin getOwningPlugin() {
            return this.plugin;
        }

        @Override
        public boolean isRepeatingTask() {
            return false;
        }

        @Override
        public CancelledState cancel() {
            return CancelledState.CANCELLED_BY_CALLER;
        }

        @Override
        public ExecutionState getExecutionState() {
            return ExecutionState.FINISHED;
        }
    }
}
