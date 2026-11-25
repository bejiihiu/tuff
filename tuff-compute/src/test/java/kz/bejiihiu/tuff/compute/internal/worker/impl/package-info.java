/**
 * Tests for the worker pool implementation.
 *
 * <p>Everything here drives the real {@link kz.bejiihiu.tuff.compute.internal.worker.impl.DynamicWorkerPool}
 * through its public contract. The loop and the queue are test doubles because the engine and the
 * queue implementation are separate components; they behave like the real ones: the loop catches
 * task throwables and returns {@code false} when the queue is empty.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.worker.impl;

import org.jspecify.annotations.NullMarked;
