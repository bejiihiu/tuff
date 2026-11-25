/**
 * The engine: submission validation, task handles, consumers, dispatch and lifecycle.
 *
 * <p>This package glues every other internal component together and is the only place allowed to
 * know all of them. It owns the {@link kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue}, the
 * {@link kz.bejiihiu.tuff.compute.internal.worker.WorkerPool}, the
 * {@link kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector} and the
 * {@link kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveConcurrencyController}, and it
 * implements {@link kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop} on top of
 * {@link kz.bejiihiu.tuff.compute.internal.engine.Dispatcher}.
 *
 * <p>Everything a submission touches before it is queued lives here: consumer lookup, policy checks,
 * rejection, id assignment, handle creation. After that the queue takes over, and after a poll the
 * engine again owns the task until its handle is completed.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.engine;

import org.jspecify.annotations.NullMarked;
