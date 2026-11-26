/**
 * Public contracts of the Tuff unified thread management infrastructure.
 *
 * <p>UTMI is the one place that knows which thread pools the server has, how many threads they own,
 * how loaded they are and which CPU budget is left after the critical lanes took their share. It does
 * not schedule game work: Folia keeps owning regions, ticks, order and region ownership, and the
 * compute engine keeps owning background task ordering. This package only describes the execution
 * layer both of them run on.
 *
 * <p>What is supported API:
 * <ul>
 *     <li>reading configuration and snapshots through {@link kz.bejiihiu.tuff.threads.UnifiedThreadManager};</li>
 *     <li>registering a pool description through {@link kz.bejiihiu.tuff.threads.ExecutionPoolRegistry};</li>
 *     <li>creating threads for a registered pool through
 *     {@link kz.bejiihiu.tuff.threads.ThreadFactoryProvider};</li>
 *     <li>acquiring and releasing a slot or core lease through
 *     {@link kz.bejiihiu.tuff.threads.ThreadResourceController}.</li>
 * </ul>
 *
 * <p>Nothing here mutates a running thread's affinity, priority or scheduling class, and nothing here
 * can move work between pools. Registration is not merging: a pool keeps its own threads, its own
 * ordering guarantees and its own lifecycle, exactly as described by
 * {@link kz.bejiihiu.tuff.threads.PoolSpec}.
 *
 * <p>Threading: every type in this package is immutable or thread safe and may be read from any
 * thread, including region tick threads. Snapshot methods are cheap and allocation bounded; they must
 * still not be called in a hot loop on a tick thread. Mutating operations (registration, factory,
 * lease) are thread safe but are meant for bootstrap and subsystem owners, not for per tick code.
 * Lease acquisition from a thread that belongs to the {@code region-execution} pool is rejected with
 * {@link java.lang.IllegalStateException}: a tick thread is the resource it would be waiting for.
 *
 * <p>Lifecycle: the manager is created and started once by the server bootstrap and stopped near the
 * end of the shutdown sequence. Creating an instance, starting it and shutting it down is an internal
 * contract (see the {@code internal} package tree), because a plugin must never own the thread layer.
 * After shutdown the manager is read only: registrations and leases are refused, snapshots keep the
 * last observed values.
 *
 * <p>Everything under {@code kz.bejiihiu.tuff.threads.internal} carries no compatibility promise. It
 * exists so the implementations of this contract can be split between packages and written in
 * parallel; it must not be imported by plugins, and server code touches it only through the single
 * bootstrap seam described there.
 */
@NullMarked
package kz.bejiihiu.tuff.threads;

import org.jspecify.annotations.NullMarked;
