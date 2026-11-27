package kz.bejiihiu.tuff.threads.observe;

/**
 * The observation sink that writes the {@link TickEvent} into a flight recording.
 *
 * <p>One instance is enough: it holds no state and the event's own {@code isEnabled()} check decides
 * whether a recording keeps the row. The tick path only reaches it when the configuration installed it.
 */
public final class JfrTickObserver implements TickObserver {

    /**
     * Shared sink, installed when the configuration arms per-tick recording.
     */
    public static final JfrTickObserver INSTANCE = new JfrTickObserver();

    private JfrTickObserver() {
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
        TickEvent.emit(
            origin,
            regionId,
            tickId,
            scheduledStartNanos,
            startNanos,
            endNanos,
            periodsAhead,
            tickCount
        );
    }
}
