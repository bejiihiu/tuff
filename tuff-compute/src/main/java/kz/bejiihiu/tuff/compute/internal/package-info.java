/**
 * Internal contracts of the compute scheduler.
 *
 * <p>Nothing in this package tree is supported API. The types here are {@code public} only because
 * the scheduler's own components live in separate packages and have to compose: a queue
 * implementation is consumed by the engine, the engine is consumed by the worker pool and so on.
 * Code outside {@code kz.bejiihiu.tuff.compute.internal} must never import them, and their
 * signatures may change with any refactor of the scheduler.
 *
 * <p>Layering, strictly one way:
 * <ul>
 *     <li>{@code internal.util} — clock, preconditions, throwable and thread helpers, no scheduler
 *     knowledge at all;</li>
 *     <li>{@code internal.queue} — queue entries, the scheduling queue, per consumer quotas;</li>
 *     <li>{@code internal.metrics} — counters, gauges and JFR events;</li>
 *     <li>{@code internal.adaptive} — concurrency controller;</li>
 *     <li>{@code internal.worker} — worker pool lifecycle and the loop it calls;</li>
 *     <li>{@code internal.engine} — submission validation, handles, dispatch, wiring of everything
 *     above.</li>
 * </ul>
 *
 * <p>Dependencies may only point downwards in that list, and only the engine may depend on all of
 * them. The queue never calls the engine, the metrics never call the queue, and the adaptive
 * controller never calls the pool.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal;

import org.jspecify.annotations.NullMarked;
