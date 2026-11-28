package kz.bejiihiu.tuff.threads.idle;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import kz.bejiihiu.tuff.threads.ResourceMetrics;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceMetricsImpl;
import org.jspecify.annotations.Nullable;

/**
 * The thread layer's own periodic snapshot refresh, offered as idle-safe work.
 *
 * <p>{@code Tuff Threads Sampler} wakes once a second and calls
 * {@link UnifiedThreadManager#snapshot()} so {@code /tuff threads} and the tick pressure monitor read a
 * published sample instead of measuring on the console thread. That pass is the producer this class
 * exposes: one bounded walk over the registered threads, one probe call per pool, one CPU read per
 * thread and a handful of JVM counters. It takes no pool lock, does no I/O and never touches world
 * state, which is exactly the {@link IdleWork} contract.
 *
 * <p>Only the pass moves. The refresher keeps everything that is not safe on a tick thread: the
 * unregistered thread scan and the region walk stay on that thread, and it keeps running its one
 * second loop as the fallback, so a server whose fund is fully busy refreshes exactly as it does
 * today.
 *
 * <p>The pass is forced rather than requested through the public snapshot call. That call is
 * throttled to one pass per configured interval, and the refresher keeps that interval exactly
 * filled, so a second driver asking politely would always read the cache and never move anything.
 * The forced pass is the internal sampler behind the public view; the region part of that one pass
 * answers from the refresher's last list (a tick thread must not take the regioniser lock), which is
 * why the refresher still keeps four passes out of five.
 *
 * <p>Pacing is deliberately slower than the refresher: one offer per {@value #WARM_INTERVAL_MILLIS}
 * milliseconds. The region probe answers from its last list when it is called from a tick thread, so
 * if idle work took every pass the region part of the sample would stop moving. Letting the refresher
 * keep four passes out of five keeps the region view within about one interval while still moving
 * real work onto an idle fund.
 */
public final class MetricsIdleSource implements IdleWorkSource {

    /**
     * Shortest gap between two passes handed to idle work.
     */
    static final long WARM_INTERVAL_MILLIS = 5_000L;

    private static final long WARM_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(WARM_INTERVAL_MILLIS);

    private final BooleanSupplier running;
    private final BooleanSupplier enabled;
    private final Runnable refresh;
    private final LongSupplier clock;
    private final long intervalNanos;

    /** When the last pass was offered, in clock terms; {@link Long#MIN_VALUE} before the first one. */
    private final AtomicLong lastOfferNanos = new AtomicLong(Long.MIN_VALUE);

    /**
     * Creates the source over a running thread layer.
     *
     * @param manager thread layer whose snapshot pass is handed out; never {@code null}
     * @throws NullPointerException if {@code manager} is {@code null}
     */
    public MetricsIdleSource(final UnifiedThreadManager manager) {
        this(
            manager::isRunning,
            () -> manager.metrics().enabled(),
            forcedPass(manager),
            System::nanoTime,
            WARM_INTERVAL_NANOS
        );
    }

    /**
     * Creates the source with explicit seams, for tests.
     *
     * @param running       whether the layer may still be sampled
     * @param enabled       whether metrics collection is on
     * @param refresh       the pass itself
     * @param clock         time source for the offer gap
     * @param intervalNanos shortest gap between two offers
     */
    MetricsIdleSource(
        final BooleanSupplier running,
        final BooleanSupplier enabled,
        final Runnable refresh,
        final LongSupplier clock,
        final long intervalNanos
    ) {
        this.running = running;
        this.enabled = enabled;
        this.refresh = refresh;
        this.clock = clock;
        this.intervalNanos = intervalNanos;
    }

    /**
     * Returns the pass to run, forced when the public view is backed by the internal sampler.
     *
     * <p>The fallback is the public snapshot call. It is correct, just throttled: a server that
     * replaced the metrics view with something other than the module's own implementation still gets
     * a refresh, it may simply be the cached one of the current interval.
     *
     * @param manager thread layer
     * @return a runnable that takes one metrics pass
     */
    private static Runnable forcedPass(final UnifiedThreadManager manager) {
        final ResourceMetrics metrics = manager.metrics();
        if (metrics instanceof ResourceMetricsImpl impl) {
            return impl.sampler()::sampleNow;
        }
        return manager::snapshot;
    }

    /**
     * Offers one metrics pass when the warm gap has passed, or {@code null} otherwise.
     *
     * <p>The timestamp moves at offer time, not at completion, so two idle runners racing for the
     * same window produce one pass and one {@code null}. A stopped layer and a layer with collection
     * disabled are both inert: there is nothing to refresh and the structural snapshot they would
     * return is not worth a tick thread.
     *
     * <p>The pass itself is a single atomic sample of the thread layer: it holds the sampler's own lock
     * for the whole walk and there is no cancellation point inside it. The returned unit therefore
     * checks {@code canContinue} before it starts a pass and again after it returns - a slice that is
     * already spent never starts a pass, and a pass that was started cannot be stopped half way. The
     * bound on the overrun is one pass, which is the shortest unit this layer can offer; a caller that
     * needs a smaller bound must not hand out this producer.
     *
     * @return the pass, or {@code null} when not due
     */
    @Override
    public @Nullable IdleWork poll() {
        if (!this.running.getAsBoolean() || !this.enabled.getAsBoolean()) {
            return null;
        }
        final long now = this.clock.getAsLong();
        final long last = this.lastOfferNanos.get();
        if (last != Long.MIN_VALUE && now >= last && now - last < this.intervalNanos) {
            return null;
        }
        if (!this.lastOfferNanos.compareAndSet(last, now)) {
            return null;
        }
        return canContinue -> {
            if (!canContinue.getAsBoolean()) {
                // the slice is spent: do not start work that has no cancellation point
                return;
            }
            this.refresh.run();
            // no way to abandon a pass that is already running; the answer is only read so a future
            // sliced implementation has the same shape and the overrun stays one pass by construction
            canContinue.getAsBoolean();
        };
    }
}
