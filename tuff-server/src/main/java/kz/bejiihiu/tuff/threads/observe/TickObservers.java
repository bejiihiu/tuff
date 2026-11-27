package kz.bejiihiu.tuff.threads.observe;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * The switch between a tick body and its observers.
 *
 * <p>The tick path reads one volatile reference and checks it for {@code null}. That is the whole cost
 * of observation being off: no lookup, no event object, no region walk. The reference is installed once
 * when the configuration is read and cleared on shutdown, so the value does not change while ticks run.
 *
 * <p>The reference is an effective observer built from two layers: the base observer, which belongs to
 * the configuration that owns per-tick recording, and any number of listeners, which independent
 * subsystems add while the server runs. Composing them here keeps the tick path at one read and no
 * allocation: the effective observer is rebuilt only when a layer changes, never per tick.
 *
 * <p>A listener is diagnostics on top of a tick, so a failing listener cannot break the feed. Its
 * exception is logged once, the base observer and the other listeners still run, and the tick finishes.
 */
public final class TickObservers {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Threads");

    private static final Object LOCK = new Object();

    /**
     * Registered listeners, in registration order. Copy on write: the tick path iterates a snapshot
     * that is already built, while registration happens rarely and never on a tick thread.
     */
    private static final CopyOnWriteArrayList<Listener> LISTENERS = new CopyOnWriteArrayList<>();

    private static final AtomicBoolean BASE_FAILURE_REPORTED = new AtomicBoolean();

    private static volatile @Nullable TickObserver installed;
    private static volatile @Nullable TickObserver effective;

    private TickObservers() {
    }

    /**
     * Returns the observer the tick body must call.
     *
     * <p>This is the one volatile read the tick body pays. It is intentionally a plain getter so a
     * caller can keep the reference for the duration of one observation instead of reading twice. The
     * value is either the base observer alone, a fan-out object when listeners are registered, or
     * {@code null} when nothing is installed.
     *
     * @return the effective observer, or {@code null} when observation is off
     */
    public static @Nullable TickObserver observer() {
        return effective;
    }

    /**
     * Installs the base observer that receives every following tick body.
     *
     * <p>Listeners are not affected: a re-install replaces the base half of the feed and keeps every
     * subsystem that registered through {@link #addListener(TickObserver)}.
     *
     * @param newObserver observer to install
     * @throws NullPointerException if the observer is {@code null}
     */
    public static void install(final TickObserver newObserver) {
        Objects.requireNonNull(newObserver, "newObserver");
        synchronized (LOCK) {
            installed = newObserver;
            rebuild();
        }
    }

    /**
     * Stops observation and drops every registered listener.
     *
     * <p>Every tick body after this call pays one volatile read and nothing else. Clearing the
     * listeners as well is what makes a stopped server leave nothing behind; a subsystem that keeps
     * running after this point must register again.
     */
    public static void uninstall() {
        synchronized (LOCK) {
            installed = null;
            LISTENERS.clear();
            effective = null;
        }
    }

    /**
     * Adds a listener that receives every tick body after the base observer.
     *
     * <p>Registration is idempotent by listener identity and never runs on a tick thread. The listener
     * runs on the tick thread that produced the body, in registration order, after the base observer.
     *
     * @param listener listener to add
     * @throws NullPointerException if the listener is {@code null}
     */
    public static void addListener(final TickObserver listener) {
        Objects.requireNonNull(listener, "listener");
        synchronized (LOCK) {
            for (final Listener current : LISTENERS) {
                if (current.observer == listener) {
                    return;
                }
            }
            LISTENERS.add(new Listener(listener));
            rebuild();
        }
    }

    /**
     * Removes a previously added listener.
     *
     * <p>Removing a listener that was never added is a no-op, so a shutdown path can call this without
     * tracking whether the start path got far enough to register.
     *
     * @param listener listener to remove
     * @throws NullPointerException if the listener is {@code null}
     */
    public static void removeListener(final TickObserver listener) {
        Objects.requireNonNull(listener, "listener");
        synchronized (LOCK) {
            if (LISTENERS.removeIf(current -> current.observer == listener)) {
                rebuild();
            }
        }
    }

    private static void rebuild() {
        final TickObserver base = installed;
        final Listener[] listeners = LISTENERS.toArray(new Listener[0]);
        effective = listeners.length == 0 ? base : new FanOut(base, listeners);
    }

    private static void reportBaseFailure(final Throwable failure) {
        if (BASE_FAILURE_REPORTED.compareAndSet(false, true)) {
            LOGGER.warn("The installed tick observer threw; the listeners keep running", failure);
        } else {
            LOGGER.debug("The installed tick observer threw again", failure);
        }
    }

    /**
     * One registered listener and its own failure latch, so a broken listener is reported once.
     */
    private static final class Listener {

        private final TickObserver observer;
        private final AtomicBoolean failureReported = new AtomicBoolean();

        private Listener(final TickObserver observer) {
            this.observer = observer;
        }

        private void deliver(
            final TickOrigin origin,
            final long regionId,
            final long tickId,
            final long scheduledStartNanos,
            final long startNanos,
            final long endNanos,
            final long periodsAhead,
            final long tickCount
        ) {
            try {
                this.observer.onTick(
                    origin,
                    regionId,
                    tickId,
                    scheduledStartNanos,
                    startNanos,
                    endNanos,
                    periodsAhead,
                    tickCount
                );
            } catch (final Throwable failure) {
                if (this.failureReported.compareAndSet(false, true)) {
                    LOGGER.warn("A tick observer listener threw; it is isolated and the other listeners keep running", failure);
                } else {
                    LOGGER.debug("A tick observer listener threw again; it stays isolated", failure);
                }
            }
        }
    }

    /**
     * The composed observer: base first, then the listeners in registration order.
     */
    private static final class FanOut implements TickObserver {

        private final @Nullable TickObserver base;
        private final Listener[] listeners;

        private FanOut(final @Nullable TickObserver base, final Listener[] listeners) {
            this.base = base;
            this.listeners = listeners;
        }

        @Override
        public void onTick(
            final TickOrigin origin,
            final long regionId,
            final long tickId,
            final long scheduledStartNanos,
            final long startNanos,
            final long endNanos,
            final long periodsAhead,
            final long tickCount
        ) {
            final TickObserver first = this.base;
            if (first != null) {
                try {
                    first.onTick(origin, regionId, tickId, scheduledStartNanos, startNanos, endNanos, periodsAhead, tickCount);
                } catch (final Throwable failure) {
                    reportBaseFailure(failure);
                }
            }
            for (final Listener listener : this.listeners) {
                listener.deliver(origin, regionId, tickId, scheduledStartNanos, startNanos, endNanos, periodsAhead, tickCount);
            }
        }
    }
}
