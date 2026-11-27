package kz.bejiihiu.tuff.threads.observe;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The switch between a tick body and its observer.
 *
 * <p>The tick path reads one volatile reference and checks it for {@code null}. That is the whole cost
 * of observation being off: no lookup, no event object, no region walk. The reference is installed once
 * when the configuration is read and cleared on shutdown, so the value does not change while ticks run.
 */
public final class TickObservers {

    private static volatile @Nullable TickObserver observer;

    private TickObservers() {
    }

    /**
     * Returns the installed observer.
     *
     * <p>This is the one volatile read the tick body pays. It is intentionally a plain getter so a
     * caller can keep the reference for the duration of one observation instead of reading twice.
     *
     * @return the observer, or {@code null} when observation is off
     */
    public static @Nullable TickObserver observer() {
        return observer;
    }

    /**
     * Installs the observer that receives every following tick body.
     *
     * @param newObserver observer to install
     * @throws NullPointerException if the observer is {@code null}
     */
    public static void install(final TickObserver newObserver) {
        observer = Objects.requireNonNull(newObserver, "newObserver");
    }

    /**
     * Stops observation. Every tick body after this call pays one volatile read and nothing else.
     */
    public static void uninstall() {
        observer = null;
    }
}
