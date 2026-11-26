/**
 * Internal contracts of the unified thread manager.
 *
 * <p>Nothing under this package tree is supported API. The types are {@code public} only because the
 * implementation is split into packages that have to compose: the registry is consumed by the metrics
 * sampler, the sampler is consumed by the runtime, the runtime is consumed by the single bootstrap seam.
 * Plugins must never import them, and their signatures may change with any refactor.
 *
 * <p>Layering, strictly one way:
 * <ul>
 *     <li>{@code internal.spi} - the contracts between the public facade and the implementation, plus
 *     the few probes an integrated pool implements;</li>
 *     <li>{@code internal.factory} - name policy, thread creation and adoption;</li>
 *     <li>{@code internal.registry} - live thread bookkeeping, counters and leak detection;</li>
 *     <li>{@code internal.topology} - processor, container and NUMA detection, budget planning;</li>
 *     <li>{@code internal.resources} - admission, leases and the shared budget;</li>
 *     <li>{@code internal.pools} - registry storage and probe attachment;</li>
 *     <li>{@code internal.metrics} - sampling, snapshots and JFR events;</li>
 *     <li>{@code internal.adaptive} - pressure driven adjustments for non critical pools;</li>
 *     <li>{@code internal.guard} and {@code internal.scan} - static and runtime enforcement;</li>
 *     <li>{@code internal.runtime} - wiring of everything above behind one factory.</li>
 * </ul>
 *
 * <p>Dependencies point downwards in that list. A probe never calls the metrics layer, the metrics layer
 * never calls the runtime, and only the runtime may depend on all of them. The public package depends on
 * nothing internal.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal;

import org.jspecify.annotations.NullMarked;
