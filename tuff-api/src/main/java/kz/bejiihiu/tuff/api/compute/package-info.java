/**
 * Plugin-facing access to the Tuff compute engine.
 *
 * <p>The scheduler itself lives in {@code kz.bejiihiu.tuff.compute} and knows nothing about
 * Minecraft, Bukkit or Folia. This package adds the two pieces a plugin or a server subsystem
 * actually needs:
 *
 * <ul>
 *   <li>{@link kz.bejiihiu.tuff.api.compute.TuffCompute} — the process wide accessor for the engine
 *       that the server bootstrap installs once.</li>
 *   <li>{@link kz.bejiihiu.tuff.api.compute.ComputeHandoff} — helpers that move a finished compute
 *       result back onto the region, entity or global context that is allowed to touch the
 *       world.</li>
 *   <li>{@link kz.bejiihiu.tuff.api.compute.ComputeExecutor} — an {@link java.util.concurrent.Executor}
 *       adapter for APIs that only accept an executor.</li>
 * </ul>
 *
 * <p>Compute tasks run on scheduler workers that own no region and no entity. Never read or write
 * world state from a task body; compute a self contained snapshot and deliver the result through
 * {@link kz.bejiihiu.tuff.api.compute.ComputeHandoff}.
 *
 * <p>When the engine is disabled in the server configuration, {@link
 * kz.bejiihiu.tuff.api.compute.TuffCompute#scheduler()} is empty. Plugins that can work without it
 * should fall back to their own code path; plugins that cannot should say so during enable.
 */
@NullMarked
package kz.bejiihiu.tuff.api.compute;

import org.jspecify.annotations.NullMarked;
