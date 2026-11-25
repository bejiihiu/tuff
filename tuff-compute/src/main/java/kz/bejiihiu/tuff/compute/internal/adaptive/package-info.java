/**
 * Adaptive concurrency control.
 *
 * <p>The controller watches how tasks behave and moves the pool wide concurrency limit up or down.
 * It exists because a fixed worker count is a guess: on a busy server the same pool can be
 * underutilised at one moment and thrashing the memory subsystem the next. The controller only ever
 * produces a number; applying it to the worker pool is the engine's job.
 *
 * <p>Invariants every implementation must keep:
 * <ul>
 *     <li>the limit never leaves {@code [minLimit, maxLimit]};</li>
 *     <li>it never reaches zero, so the pool always makes progress;</li>
 *     <li>it changes at most once per sample window and only after enough samples arrived;</li>
 *     <li>a disabled controller never changes anything, {@link #currentLimit()} simply passes the
 *     configured limit through.</li>
 * </ul>
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.adaptive;

import org.jspecify.annotations.NullMarked;
