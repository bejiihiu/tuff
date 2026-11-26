package kz.bejiihiu.tuff.api.compute;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.Plugin;
import org.jspecify.annotations.Nullable;

/**
 * The three scheduler calls {@link ComputeHandoff} needs, behind a seam.
 *
 * <p>This is package private on purpose and is not part of the plugin API. It exists so the
 * delivery paths can be exercised without a running server: a test swaps it for a double that
 * records the scheduled work instead of handing it to a region thread. Production always uses
 * {@link #bukkit()}.
 *
 * <p>Every method has to be safe to call from the thread that completed the compute future. It
 * only schedules work; the callback itself is never invoked on that thread.
 */
interface HandoffBackend {

    /**
     * Schedules {@code action} on the region that owns a chunk, on the next tick.
     *
     * @param plugin plugin that owns the delivery
     * @param world  world of the target region
     * @param chunkX chunk x coordinate of the target region
     * @param chunkZ chunk z coordinate of the target region
     * @param action work to run on the owning region thread
     * @return the scheduled task, or {@code null} when the scheduler accepted nothing
     */
    @Nullable ScheduledTask scheduleInRegion(
        Plugin plugin,
        World world,
        int chunkX,
        int chunkZ,
        Runnable action
    );

    /**
     * Schedules {@code action} on the scheduler of an entity, on the next tick.
     *
     * @param plugin  plugin that owns the delivery
     * @param entity  entity whose owning region should run the action
     * @param action  work to run on the region that owns the entity
     * @param retired callback to run if the entity is removed before the action can run
     * @return the scheduled task, or {@code null} when the entity is already retired
     */
    @Nullable ScheduledTask scheduleOnEntity(
        Plugin plugin,
        Entity entity,
        Runnable action,
        Runnable retired
    );

    /**
     * Schedules {@code action} on the global region, on the next tick.
     *
     * @param plugin plugin that owns the delivery
     * @param action work to run on the global region thread
     * @return the scheduled task, or {@code null} when the scheduler accepted nothing
     */
    @Nullable ScheduledTask scheduleInGlobal(Plugin plugin, Runnable action);

    /**
     * Returns the backend that schedules through the running server's region, entity and global
     * schedulers.
     *
     * @return the production backend
     */
    static HandoffBackend bukkit() {
        return BukkitHandoffBackend.INSTANCE;
    }
}
