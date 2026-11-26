/**
 * Contracts between the public facade and the implementation of the thread manager.
 *
 * <p>These interfaces are the seams that let the implementation be written in parallel: one package
 * owns the thread factory, another the registry, another the budget, another the metrics sampler, and
 * the runtime only wires them. They are not a plugin API and carry no compatibility promise.
 *
 * <p>Rules:
 * <ul>
 *     <li>a contract here never references Minecraft, Bukkit, Paper or Folia types, so the module stays
 *     plain Java;</li>
 *     <li>implementations must be safe to call from any thread and must not block on a pool they serve;</li>
 *     <li>an implementation reports an unsupported operation with
 *     {@link java.lang.UnsupportedOperationException} or a documented degraded reason, never with a
 *     silent no-op;</li>
 *     <li>probes and sinks must be cheap and non throwing: a broken observer must not be able to break
 *     the server's thread layer.</li>
 * </ul>
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.spi;

import org.jspecify.annotations.NullMarked;
