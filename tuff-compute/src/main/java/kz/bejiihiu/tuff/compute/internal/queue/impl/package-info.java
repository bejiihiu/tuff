/**
 * Default implementations of the scheduling queue and the per consumer quota controller.
 *
 * <p>Everything in this package is internal plumbing: the engine constructs it, the workers call
 * it, and nothing outside the compute module is allowed to reference these classes directly.
 * Implementations here are safe to share between arbitrary producer and consumer threads.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.queue.impl;

import org.jspecify.annotations.NullMarked;
