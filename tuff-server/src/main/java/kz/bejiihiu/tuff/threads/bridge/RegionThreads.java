package kz.bejiihiu.tuff.threads.bridge;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ThreadFactoryProvider;
import kz.bejiihiu.tuff.threads.ThreadSpec;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * The one call site Folia's region scheduler needs to hand its tick threads to the thread layer.
 *
 * <p>Region execution is the most constrained lane: the concrete thread class, its group, its daemon
 * flag and its uncaught handler belong to Folia, and changing any of them breaks ownership checks,
 * AsyncCatcher and the shutdown contract. So the integration borrows accounting only: the factory
 * still builds its own {@code TickThreadRunner}, and the {@link Runnable} it hands over is passed
 * through {@link ThreadFactoryProvider#wrap} when the region pool is at L2 or deeper.
 *
 * <p>{@link #wrapBody(Runnable)} is called from inside the scheduler's thread factory, i.e. during
 * startup, before the server accepts players. It has one hard rule: never throw. A wrapper that fails
 * to build must degrade to the original body, because the alternative is a scheduler without threads
 * and a server that does not tick. Failures are logged once, not per thread.
 *
 * <p>The wrapper itself does not touch the exception path: it registers the thread, runs the body and
 * unregisters it in a {@code finally}. A {@link Throwable} from the body still reaches Folia's
 * uncaught handler, which is what halts the scheduler and stops the server.
 *
 * <p>The holder is process wide because the scheduler has no instance to hand us: {@link #install}
 * runs from the thread bootstrap, before Folia's region scheduler is constructed, and {@link #clear}
 * runs on shutdown so a stopped layer cannot keep receiving bodies.
 */
public final class RegionThreads {

    private static final Logger LOGGER = LogManager.getLogger("Tuff/Threads");

    private static final AtomicReference<@Nullable Binding> BINDING = new AtomicReference<>();

    /**
     * At most one warning per process: a factory that logs on every thread turns a broken start into
     * a wall of text.
     */
    private static final AtomicBoolean WARNED = new AtomicBoolean();

    /**
     * Documentation only: {@link ThreadFactoryProvider#wrap} reads the pool and nothing else. The
     * real name, group, daemon flag and handler are applied by Folia's own factory.
     */
    private static final ThreadSpec REGION_SPEC = new ThreadSpec(
        PoolId.REGION_EXECUTION,
        "Folia Region Scheduler Thread",
        false,
        Thread.NORM_PRIORITY,
        (thread, failure) -> { },
        Thread.currentThread().getThreadGroup()
    );

    private RegionThreads() {
    }

    /**
     * Binds the bridge to a running thread layer.
     *
     * <p>Only two things make the bridge active: {@code thread-management.region-execution.managed}
     * is on, and the region pool is registered at {@link kz.bejiihiu.tuff.threads.CoverageLevel#L2}
     * or deeper. Anything else - a disabled layer, a failed start, an observe-only configuration, a
     * pool registered at L1 - clears the binding and leaves Folia exactly as it was.
     *
     * <p>Never throws: the caller runs inside server startup and a broken bridge must not decide
     * whether the server boots.
     *
     * @param manager running thread manager, or {@code null} to clear the bridge
     */
    public static void install(final @Nullable UnifiedThreadManager manager) {
        BINDING.set(null);
        if (manager == null) {
            return;
        }
        try {
            if (!manager.settings().regionExecution().managed()) {
                return;
            }
            final Optional<PoolSpec> spec = manager.pools().spec(PoolId.REGION_EXECUTION);
            if (spec.isEmpty() || !spec.get().coverage().managesThreads()) {
                warnOnce(
                    "region-execution.managed is on, but the pool is registered as "
                        + spec.map(pool -> pool.coverage().toString()).orElse("absent")
                        + "; Folia's region threads stay unaccounted (register the pool at L2 before the scheduler starts)",
                    null
                );
                return;
            }
            BINDING.set(new Binding(manager.threadFactory(), REGION_SPEC));
        } catch (final Throwable failure) {
            BINDING.set(null);
            warnOnce("could not install the region thread bridge; region threads stay unaccounted", failure);
        }
    }

    /**
     * Unbinds the bridge, so bodies created after this point run as they always did.
     *
     * <p>Called during shutdown, before the manager starts refusing work. Threads that were wrapped
     * earlier keep their wrapper; it unregisters on exit, which the registry allows after stop.
     */
    public static void clear() {
        BINDING.set(null);
    }

    /**
     * Wraps a region tick thread body for accounting, or returns it untouched.
     *
     * <p>The returned runnable keeps the thread's class, name, group, daemon flag and handler; it only
     * registers the thread under {@code region-execution} for the duration of the body. When no layer
     * is installed, or the provider refuses, the original instance is returned and the server keeps
     * ticking without accounting.
     *
     * <p>Never throws. The wrapper does not catch the body's own failures.
     *
     * @param run the body Folia's thread factory was given
     * @return the wrapped body, or {@code run} itself
     * @throws NullPointerException if {@code run} is {@code null}
     */
    public static Runnable wrapBody(final Runnable run) {
        Objects.requireNonNull(run, "run");
        final Binding current = BINDING.get();
        if (current == null) {
            return run;
        }
        try {
            return current.provider().wrap(current.spec(), run);
        } catch (final Throwable failure) {
            // a provider that refuses must not cost the server its tick threads
            warnOnce("could not wrap a region thread body; the thread runs unaccounted", failure);
            return run;
        }
    }

    /**
     * Reports whether a body handed in now would be wrapped, for diagnostics and tests.
     *
     * @return {@code true} when the bridge has an active binding
     */
    static boolean installed() {
        return BINDING.get() != null;
    }

    private static void warnOnce(final String message, final @Nullable Throwable failure) {
        if (!WARNED.compareAndSet(false, true)) {
            return;
        }
        if (failure == null) {
            LOGGER.warn(message);
        } else {
            LOGGER.warn(message, failure);
        }
    }

    /**
     * What a wrapped body needs: the provider that does the accounting and the spec describing the
     * pool. Immutable, so a swap is a single reference assign.
     *
     * @param provider thread factory provider of the running layer
     * @param spec     spec of the region pool, only its pool id is read by the provider
     */
    private record Binding(ThreadFactoryProvider provider, ThreadSpec spec) {
    }
}
