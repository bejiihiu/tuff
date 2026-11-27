package kz.bejiihiu.tuff.threads.idle;

import org.jspecify.annotations.Nullable;

/**
 * Source of {@link IdleWork} for a tick thread that has no permit and no due tick.
 *
 * <p>Polling is free to be frequent: a tick thread calls {@link #poll()} at most once per slice and
 * only while it is parked, so an implementation should answer {@code null} cheaply while it has
 * nothing due. A source decides its own pacing; the runtime does not add a timer to it.
 *
 * <p>A source is a seam, not a queue with delivery guarantees. A job it returns may be dropped when
 * the tick thread is granted a tick before the job starts, and a job is not retried by the runtime;
 * a source that wants continuation keeps its own state and offers the next step on the next poll.
 *
 * <p>Threading: polled from any number of tick threads at once, and the returned job runs on the
 * polling thread. The implementation must be safe for that and must not block inside
 * {@link #poll()}.
 *
 * <p>Without a source the idle path is inert: the scheduler starts no work and reports no source in
 * its snapshot, which is what a server without the thread layer gets.
 */
@FunctionalInterface
public interface IdleWorkSource {

    /**
     * Returns the next unit of idle-safe work, or {@code null} when nothing is due.
     *
     * @return a job to run on the calling tick thread, or {@code null}
     */
    @Nullable IdleWork poll();
}
