package kz.bejiihiu.tuff.api.compute;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.Plugin;
import org.jspecify.annotations.Nullable;

/**
 * Production {@link HandoffBackend}: delegates to the Folia schedulers of the running server.
 *
 * <p>Nothing is cached here. The scheduler objects are looked up per call, so this backend stays
 * valid across a server restart within the same JVM and never captures a world or an entity.
 */
final class BukkitHandoffBackend implements HandoffBackend {

    static final BukkitHandoffBackend INSTANCE = new BukkitHandoffBackend();

    private BukkitHandoffBackend() {
    }

    @Override
    public @Nullable ScheduledTask scheduleInRegion(
        final Plugin plugin,
        final World world,
        final int chunkX,
        final int chunkZ,
        final Runnable action
    ) {
        return Bukkit.getRegionScheduler().run(plugin, world, chunkX, chunkZ, task -> action.run());
    }

    @Override
    public @Nullable ScheduledTask scheduleOnEntity(
        final Plugin plugin,
        final Entity entity,
        final Runnable action,
        final Runnable retired
    ) {
        return entity.getScheduler().run(plugin, task -> action.run(), retired);
    }

    @Override
    public @Nullable ScheduledTask scheduleInGlobal(final Plugin plugin, final Runnable action) {
        return Bukkit.getGlobalRegionScheduler().run(plugin, task -> action.run());
    }
}
