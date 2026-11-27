/**
 * The idle-safe work contract and the server's first producer of it.
 *
 * <p>A tick thread spends most of its life parked, waiting for the next deadline. This package is the
 * only supported way to hand that thread a piece of housekeeping while it waits, without letting
 * arbitrary work into the tick path. The contract is deliberately small: {@link
 * kz.bejiihiu.tuff.threads.idle.IdleWork} is one unit and {@link
 * kz.bejiihiu.tuff.threads.idle.IdleWorkSource} is the place it comes from. Nothing that is not
 * explicitly written as {@code IdleWork} can be drained, so a background computation, a plugin task or
 * a scheduled job has no path into it.
 *
 * <p>What the implementing work promises:
 * <ul>
 *     <li>short and bounded - it finishes within the slice the caller allows and returns when the
 *     cancellation flag turns false;</li>
 *     <li>non-blocking - no locks held across work, no {@code join}, no {@code sleep}, no waiting on
 *     another thread, no synchronous API that may wait for one;</li>
 *     <li>no I/O - no file, socket, database or log write;</li>
 *     <li>no world state - it does not read or write blocks, entities, chunks or a region's task
 *     queue; the region owning the thread keeps that state to itself;</li>
 *     <li>no ordering requirement that another thread could break.</li>
 * </ul>
 *
 * <p>What the caller promises: the slice is a wall-clock bound, not a suggestion. Work that ignores
 * it is legal only in the sense that the runtime survives it, and it costs the source its place: the
 * scheduler measures the lag such work adds to a tick and disables the whole idle path when the
 * configured budget is crossed. That is the runtime side of "explicitly marked idle-safe" - the type
 * says what the author claims, the measurement checks whether the claim holds.
 *
 * <p>Threading: a source may be polled from several tick threads at once, and its job runs on one of
 * them. Implementations own their own queue and their own synchronisation; the runtime holds no lock
 * while a job runs.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.idle;

import org.jspecify.annotations.NullMarked;
