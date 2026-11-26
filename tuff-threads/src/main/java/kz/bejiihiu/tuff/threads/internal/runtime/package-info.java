/**
 * Assembly and lifecycle of the unified thread layer.
 *
 * <p>This package is the only place that knows every component at once. It builds the pool registry,
 * the thread registry, the factory, the topology probe, the resource controller, the probe registry and
 * the metrics view in one order, then exposes them through {@link kz.bejiihiu.tuff.threads.UnifiedThreadManager}
 * and drives start and shutdown through {@link kz.bejiihiu.tuff.threads.internal.spi.ThreadManagerRuntime}.
 *
 * <p>Two rules shape everything here. First, construction is side effect free: no pool is registered and
 * no thread is created until {@code start()}. Second, shutdown is bounded and one directional: pools stop
 * accepting specs, then the factory stops creating, then the registry stops accounting, then probes, then
 * the sampler and finally admission. Nothing blocks, joins or waits in that path, because it runs inside
 * the window where a slow shutdown looks exactly like a hung server.
 *
 * <p>Default pools are registered observe only. Registration makes a pool visible and measurable; it
 * never hands its threads or its queue to the manager. Coverage only rises when a subsystem owner
 * registers a deeper spec on purpose.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.runtime;

import org.jspecify.annotations.NullMarked;
