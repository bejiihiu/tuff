package kz.bejiihiu.tuff.api.compute;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.logging.Level;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.IllegalPluginAccessException;
import org.bukkit.plugin.Plugin;
import org.jspecify.annotations.Nullable;

/**
 * Delivers a finished compute result back into the server context that owns the world state.
 *
 * <p>A compute task runs on a scheduler worker and owns no region and no entity. Anything that
 * touches the world has to happen on the thread that owns the target: the region of a location, the
 * scheduler of an entity, or the global region. These helpers attach a completion callback to the
 * task future and schedule the consumer exactly there, so a normal call is one line:
 *
 * <pre>{@code
 * final CompletableFuture<Pathfinder.PathResult> route = compute.submit(() -> plan(snapshot));
 * ComputeHandoff.toEntity(this, mob, route, mob.getPathfinder()::moveTo);
 * }</pre>
 *
 * <p>The consumer runs on the owning region thread, so it may safely mutate blocks, entities and
 * inventories that belong to that region. The failure handler runs on the same thread when the
 * target is still alive; if the target disappeared first it runs on the thread that completed the
 * future, so it must not touch world state and should only log or release plugin owned resources.
 *
 * <p>Delivery rules:
 *
 * <ul>
 *   <li>A cancelled compute task delivers nothing. Cancellation is a normal outcome, not an
 *       error, and it is never reported to the failure handler.</li>
 *   <li>A failed compute task delivers the original throwable to the failure handler on the target
 *       context when possible.</li>
 *   <li>If the entity is removed before the result arrives, the entity scheduler reports the
 *       retired state and the failure handler receives a {@link HandoffException} with
 *       {@link HandoffException.Reason#ENTITY_RETIRED}. That callback runs inside the entity
 *       scheduler's retired path, which is critical region code: it must not load chunks, remove
 *       entities or mutate world state, only log or release plugin owned resources.</li>
 *   <li>If the plugin is disabled before the result can be scheduled, the failure handler receives
 *       {@link HandoffException.Reason#PLUGIN_DISABLED}. A task that was already scheduled and is
 *       then cancelled by Bukkit because the plugin got disabled is dropped silently, because the
 *       server does not invoke any callback for that case.</li>
 *   <li>If the target region or world is gone, the server discards the scheduled task. This is a
 *       silent skip by design; no result can be applied to a world that no longer exists.</li>
 * </ul>
 *
 * <p>Every call delivers at most once and never blocks or joins the future. Do not hand the same
 * future to two calls unless applying the result twice is intended.
 */
public final class ComputeHandoff {

    private static final System.Logger LOGGER = System.getLogger("TuffCompute");

    private ComputeHandoff() {
    }

    /**
     * Delivers a compute result to the region that owns {@code location}.
     *
     * <p>The world and chunk coordinates are captured immediately, before the future completes, so
     * the completion callback never reads the mutable {@link Location} from a worker thread. Failures
     * are logged through the plugin logger.
     *
     * @param plugin   plugin that owns the delivery; it must stay enabled for the result to be applied
     * @param location location whose owning region should run the consumer
     * @param result   future of the computation
     * @param apply    consumer of the result, runs on the owning region thread
     * @param <T>      type of the computed value
     * @throws NullPointerException     if an argument is {@code null} or the location has no world
     * @throws IllegalArgumentException if the location's world is not loaded
     */
    public static <T> void toRegion(
        final Plugin plugin,
        final Location location,
        final CompletionStage<T> result,
        final Consumer<? super T> apply
    ) {
        toRegion(plugin, location, result, apply, logTo(plugin));
    }

    /**
     * Delivers a compute result to the region that owns {@code location}.
     *
     * <p>This overload lets the caller decide what happens when the computation failed or the result
     * cannot be delivered. See the class documentation for the exact callback context and the
     * delivery rules.
     *
     * @param plugin    plugin that owns the delivery
     * @param location  location whose owning region should run the consumer
     * @param result    future of the computation
     * @param apply     consumer of the result, runs on the owning region thread
     * @param onFailure handler for a failed computation or a failed delivery
     * @param <T>       type of the computed value
     * @throws NullPointerException     if an argument is {@code null} or the location has no world
     * @throws IllegalArgumentException if the location's world is not loaded
     */
    public static <T> void toRegion(
        final Plugin plugin,
        final Location location,
        final CompletionStage<T> result,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        Objects.requireNonNull(location, "location");
        final World world = Objects.requireNonNull(location.getWorld(), "location must have a world");
        toRegion(plugin, world, location.getBlockX() >> 4, location.getBlockZ() >> 4, result, apply, onFailure);
    }

    /**
     * Delivers a compute result to the region that owns a chunk.
     *
     * <p>Failures are logged through the plugin logger.
     *
     * @param plugin plugin that owns the delivery
     * @param world  world of the target region
     * @param chunkX chunk x coordinate of the target region
     * @param chunkZ chunk z coordinate of the target region
     * @param result future of the computation
     * @param apply  consumer of the result, runs on the owning region thread
     * @param <T>    type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toRegion(
        final Plugin plugin,
        final World world,
        final int chunkX,
        final int chunkZ,
        final CompletionStage<T> result,
        final Consumer<? super T> apply
    ) {
        toRegion(plugin, world, chunkX, chunkZ, result, apply, logTo(plugin));
    }

    /**
     * Delivers a compute result to the region that owns a chunk.
     *
     * @param plugin    plugin that owns the delivery
     * @param world     world of the target region
     * @param chunkX    chunk x coordinate of the target region
     * @param chunkZ    chunk z coordinate of the target region
     * @param result    future of the computation
     * @param apply     consumer of the result, runs on the owning region thread
     * @param onFailure handler for a failed computation or a failed delivery
     * @param <T>       type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toRegion(
        final Plugin plugin,
        final World world,
        final int chunkX,
        final int chunkZ,
        final CompletionStage<T> result,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        Objects.requireNonNull(plugin, "plugin");
        Objects.requireNonNull(world, "world");
        handoff(result, apply, onFailure, (action, failure, originalFailure) -> {
            try {
                Bukkit.getRegionScheduler().run(plugin, world, chunkX, chunkZ, task -> action.run());
            } catch (final IllegalPluginAccessException disabled) {
                report(failure, new HandoffException(
                    HandoffException.Reason.PLUGIN_DISABLED,
                    "Plugin " + plugin.getName() + " was disabled before the compute result could be scheduled",
                    disabled
                ), originalFailure);
            } catch (final Throwable throwable) {
                report(failure, new HandoffException(
                    HandoffException.Reason.SCHEDULE_FAILED,
                    "Could not schedule the compute result on the region " + chunkX + ", " + chunkZ,
                    throwable
                ), originalFailure);
            }
        });
    }

    /**
     * Delivers a compute result to the scheduler of an entity.
     *
     * <p>Use this instead of the region variant for anything that acts on a single entity: the entity
     * scheduler follows it across regions and worlds and reports retirement if it is removed first.
     * Failures are logged through the plugin logger.
     *
     * @param plugin plugin that owns the delivery
     * @param entity entity whose owning region should run the consumer
     * @param result future of the computation
     * @param apply  consumer of the result, runs on the region that owns the entity
     * @param <T>    type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toEntity(
        final Plugin plugin,
        final Entity entity,
        final CompletionStage<T> result,
        final Consumer<? super T> apply
    ) {
        toEntity(plugin, entity, result, apply, logTo(plugin));
    }

    /**
     * Delivers a compute result to the scheduler of an entity.
     *
     * @param plugin    plugin that owns the delivery
     * @param entity    entity whose owning region should run the consumer
     * @param result    future of the computation
     * @param apply     consumer of the result, runs on the region that owns the entity
     * @param onFailure handler for a failed computation, a retired entity or a failed delivery; it
     *                  may run inside the entity retired callback, so it must not mutate world state
     * @param <T>       type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toEntity(
        final Plugin plugin,
        final Entity entity,
        final CompletionStage<T> result,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        Objects.requireNonNull(plugin, "plugin");
        Objects.requireNonNull(entity, "entity");
        handoff(result, apply, onFailure, (action, failure, originalFailure) -> {
            final Runnable retired = () -> report(failure, new HandoffException(
                HandoffException.Reason.ENTITY_RETIRED,
                "The entity was removed before the compute result could be applied"
            ), originalFailure);
            try {
                final @Nullable ScheduledTask scheduled = entity.getScheduler().run(plugin, task -> action.run(), retired);
                if (scheduled == null) {
                    retired.run();
                }
            } catch (final IllegalPluginAccessException disabled) {
                report(failure, new HandoffException(
                    HandoffException.Reason.PLUGIN_DISABLED,
                    "Plugin " + plugin.getName() + " was disabled before the compute result could be scheduled",
                    disabled
                ), originalFailure);
            } catch (final Throwable throwable) {
                report(failure, new HandoffException(
                    HandoffException.Reason.SCHEDULE_FAILED,
                    "Could not schedule the compute result on the entity scheduler",
                    throwable
                ), originalFailure);
            }
        });
    }

    /**
     * Delivers a compute result to the global region.
     *
     * <p>Use this for work that does not belong to a specific region, for example changing world
     * time or weather. Failures are logged through the plugin logger.
     *
     * @param plugin plugin that owns the delivery
     * @param result future of the computation
     * @param apply  consumer of the result, runs on the global region thread
     * @param <T>    type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toGlobal(
        final Plugin plugin,
        final CompletionStage<T> result,
        final Consumer<? super T> apply
    ) {
        toGlobal(plugin, result, apply, logTo(plugin));
    }

    /**
     * Delivers a compute result to the global region.
     *
     * @param plugin    plugin that owns the delivery
     * @param result    future of the computation
     * @param apply     consumer of the result, runs on the global region thread
     * @param onFailure handler for a failed computation or a failed delivery
     * @param <T>       type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toGlobal(
        final Plugin plugin,
        final CompletionStage<T> result,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        Objects.requireNonNull(plugin, "plugin");
        handoff(result, apply, onFailure, (action, failure, originalFailure) -> {
            try {
                Bukkit.getGlobalRegionScheduler().run(plugin, task -> action.run());
            } catch (final IllegalPluginAccessException disabled) {
                report(failure, new HandoffException(
                    HandoffException.Reason.PLUGIN_DISABLED,
                    "Plugin " + plugin.getName() + " was disabled before the compute result could be scheduled",
                    disabled
                ), originalFailure);
            } catch (final Throwable throwable) {
                report(failure, new HandoffException(
                    HandoffException.Reason.SCHEDULE_FAILED,
                    "Could not schedule the compute result on the global region",
                    throwable
                ), originalFailure);
            }
        });
    }

    /**
     * Delivers a task handle to the region that owns {@code location}.
     *
     * <p>Equivalent to {@link #toRegion(Plugin, Location, CompletionStage, Consumer)} with
     * {@code task.future()}.
     *
     * @param plugin   plugin that owns the delivery
     * @param location location whose owning region should run the consumer
     * @param task     submitted compute task
     * @param apply    consumer of the result, runs on the owning region thread
     * @param <T>      type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toRegion(
        final Plugin plugin,
        final Location location,
        final ComputeTaskHandle<T> task,
        final Consumer<? super T> apply
    ) {
        toRegion(plugin, location, Objects.requireNonNull(task, "task").future(), apply);
    }

    /**
     * Delivers a task handle to the region that owns {@code location}.
     *
     * @param plugin    plugin that owns the delivery
     * @param location  location whose owning region should run the consumer
     * @param task      submitted compute task
     * @param apply     consumer of the result, runs on the owning region thread
     * @param onFailure handler for a failed computation or a failed delivery
     * @param <T>       type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toRegion(
        final Plugin plugin,
        final Location location,
        final ComputeTaskHandle<T> task,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        toRegion(plugin, location, Objects.requireNonNull(task, "task").future(), apply, onFailure);
    }

    /**
     * Delivers a task handle to the scheduler of an entity.
     *
     * @param plugin plugin that owns the delivery
     * @param entity entity whose owning region should run the consumer
     * @param task   submitted compute task
     * @param apply  consumer of the result, runs on the region that owns the entity
     * @param <T>    type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toEntity(
        final Plugin plugin,
        final Entity entity,
        final ComputeTaskHandle<T> task,
        final Consumer<? super T> apply
    ) {
        toEntity(plugin, entity, Objects.requireNonNull(task, "task").future(), apply);
    }

    /**
     * Delivers a task handle to the scheduler of an entity.
     *
     * @param plugin    plugin that owns the delivery
     * @param entity    entity whose owning region should run the consumer
     * @param task      submitted compute task
     * @param apply     consumer of the result, runs on the region that owns the entity
     * @param onFailure handler for a failed computation, a retired entity or a failed delivery
     * @param <T>       type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toEntity(
        final Plugin plugin,
        final Entity entity,
        final ComputeTaskHandle<T> task,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        toEntity(plugin, entity, Objects.requireNonNull(task, "task").future(), apply, onFailure);
    }

    /**
     * Delivers a task handle to the global region.
     *
     * @param plugin plugin that owns the delivery
     * @param task   submitted compute task
     * @param apply  consumer of the result, runs on the global region thread
     * @param <T>    type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toGlobal(
        final Plugin plugin,
        final ComputeTaskHandle<T> task,
        final Consumer<? super T> apply
    ) {
        toGlobal(plugin, Objects.requireNonNull(task, "task").future(), apply);
    }

    /**
     * Delivers a task handle to the global region.
     *
     * @param plugin    plugin that owns the delivery
     * @param task      submitted compute task
     * @param apply     consumer of the result, runs on the global region thread
     * @param onFailure handler for a failed computation or a failed delivery
     * @param <T>       type of the computed value
     * @throws NullPointerException if an argument is {@code null}
     */
    public static <T> void toGlobal(
        final Plugin plugin,
        final ComputeTaskHandle<T> task,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure
    ) {
        toGlobal(plugin, Objects.requireNonNull(task, "task").future(), apply, onFailure);
    }

    private static <T> void handoff(
        final CompletionStage<T> result,
        final Consumer<? super T> apply,
        final Consumer<? super Throwable> onFailure,
        final Delivery delivery
    ) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(apply, "apply");
        Objects.requireNonNull(onFailure, "onFailure");
        Objects.requireNonNull(delivery, "delivery");
        result.whenComplete((value, error) -> {
            try {
                Throwable failure = unwrap(error);
                if (failure == null && value == null) {
                    failure = new NullPointerException("Compute result must not be null");
                }
                if (failure instanceof CancellationException) {
                    return;
                }
                if (failure == null) {
                    final T delivered = value;
                    delivery.schedule(() -> apply.accept(delivered), onFailure, null);
                } else {
                    final Throwable deliveredFailure = failure;
                    delivery.schedule(() -> onFailure.accept(deliveredFailure), onFailure, deliveredFailure);
                }
            } catch (final Throwable throwable) {
                LOGGER.log(System.Logger.Level.ERROR, "Unhandled failure while delivering a compute result", throwable);
            }
        });
    }

    private static @Nullable Throwable unwrap(final @Nullable Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void report(
        final Consumer<? super Throwable> onFailure,
        final HandoffException unavailable,
        final @Nullable Throwable originalFailure
    ) {
        try {
            onFailure.accept(originalFailure != null ? originalFailure : unavailable);
        } catch (final Throwable throwable) {
            LOGGER.log(System.Logger.Level.ERROR, "The compute failure handler threw", throwable);
        }
    }

    private static Consumer<Throwable> logTo(final Plugin plugin) {
        Objects.requireNonNull(plugin, "plugin");
        return throwable -> plugin.getLogger().log(
            Level.SEVERE,
            "A compute result could not be applied for plugin " + plugin.getName(),
            throwable
        );
    }

    @FunctionalInterface
    private interface Delivery {

        void schedule(Runnable action, Consumer<? super Throwable> onFailure, @Nullable Throwable originalFailure);
    }
}
