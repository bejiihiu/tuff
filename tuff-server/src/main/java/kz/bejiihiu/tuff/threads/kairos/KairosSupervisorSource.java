package kz.bejiihiu.tuff.threads.kairos;

import org.jspecify.annotations.Nullable;

/**
 * Where the supervisor thread comes from.
 *
 * <p>Preferred is the thread layer's service pool: a registered, accounted thread is not an unknown
 * thread in {@code /tuff threads} and appears in the registry like any other Tuff thread. When the
 * layer is off, the scheduler still gets a working control thread - a plain daemon with the documented
 * reason in the startup log - because losing the supervisor would silently freeze the fund.
 *
 * <p>Implementations must not throw: a source that cannot build the thread returns {@code null} and
 * the scheduler falls back to the daemon.
 */
public interface KairosSupervisorSource {

    /**
     * Creates the supervisor thread without starting it.
     *
     * @param name    base name of the thread
     * @param body    work the thread runs
     * @param handler handler for a failure that escapes the body
     * @return the thread, or {@code null} when this source cannot provide one
     */
    @Nullable Thread create(String name, Runnable body, Thread.UncaughtExceptionHandler handler);

    /**
     * Describes the source for the startup log, so a plain daemon says why it is plain.
     *
     * @return a short human readable description, never {@code null}
     */
    default String describe() {
        return "the thread layer is not running";
    }

    /**
     * Returns a source that never provides a thread, so the scheduler builds its own daemon.
     *
     * @return the plain source, never {@code null}
     */
    static KairosSupervisorSource plain() {
        return (name, body, handler) -> null;
    }
}
