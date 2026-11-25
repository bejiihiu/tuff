/**
 * Default implementation of the worker pool.
 *
 * <p>The pool is deliberately small: it owns platform threads, a concurrency gate and the
 * keep-alive/resize bookkeeping, and it knows nothing about tasks. Everything that needs a queue,
 * a dispatcher or a future is done by the {@link kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop}
 * the engine supplies.
 *
 * <p>Only the engine is expected to construct these types; the interfaces in the parent package are
 * what the rest of the scheduler sees.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.worker.impl;

import org.jspecify.annotations.NullMarked;
