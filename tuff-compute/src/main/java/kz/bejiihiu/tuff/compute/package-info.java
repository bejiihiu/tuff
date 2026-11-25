/**
 * Public API of the Tuff compute scheduler.
 *
 * <p>The scheduler owns one shared pool of worker threads and lets any server subsystem submit CPU
 * bound work to it instead of spawning its own executors. It accepts tasks, routes them to
 * registered consumers, enforces consumer quotas, prioritises and ages the queues, runs the work on
 * reusable workers, collects metrics and completes the caller's future.
 *
 * <p>The engine behind this API is deliberately free of any server dependency: nothing in this
 * package may reference Minecraft, Bukkit or Paper classes, because the compute engine is not
 * allowed to know about the world it computes things for.
 *
 * <p>Only this package is supported API. Everything under
 * {@code kz.bejiihiu.tuff.compute.internal} is an implementation contract shared between the
 * scheduler's own components, it carries no compatibility promise and must never be used from
 * outside the scheduler.
 *
 * <p>Unless stated otherwise every type here is thread safe and may be used from any thread,
 * including region threads. Task bodies however must not touch mutable world state; results are
 * handed back through the {@link java.util.concurrent.CompletableFuture} returned on submission and
 * the consumer decides how to apply them on the owning thread.
 */
@NullMarked
package kz.bejiihiu.tuff.compute;

import org.jspecify.annotations.NullMarked;
