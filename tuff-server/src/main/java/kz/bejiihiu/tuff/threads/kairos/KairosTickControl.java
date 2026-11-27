package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.Objects;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * The three questions the fund has to ask a region tick handle, and nothing else.
 *
 * <p>A {@link SchedulableTick} only promises a deadline, a tick body and a task drain. Catch-up needs
 * two facts that live inside Folia's handle next to its private
 * {@code ca.spottedleaf.common.time.Schedule}: how many tick periods the handle is behind right now,
 * and how many of them the next body may write off. The idle path needs one more: whether the region
 * was already dropped, because intermediate tasks must never be started for it. The handle exposes
 * exactly those, and this record carries them into the package without teaching the fund about
 * regions.
 *
 * <p>{@link #none()} is the answer for a tick that is not a region handle - a test double, or a
 * scheduler used outside Folia. It reports no backlog, accepts the write-off limit without doing
 * anything and never reports a cancellation, so catch-up quietly does not engage and the fund keeps
 * the stock behaviour.
 *
 * <p>All calls happen on the runner thread that holds the permit for the tick, and all must be cheap
 * and must never block.
 *
 * @param missedPeriods reader for the elapsed periods behind the deadline
 * @param writeOffLimit setter for the periods the next body may account for
 * @param cancelled     whether the region was dropped, so no work may start for it
 */
public record KairosTickControl(
    ToLongFunction<SchedulableTick> missedPeriods,
    ObjLongConsumer<SchedulableTick> writeOffLimit,
    Predicate<SchedulableTick> cancelled
) {

    /**
     * Validates the seam.
     *
     * @throws NullPointerException if a function is {@code null}
     */
    public KairosTickControl {
        Objects.requireNonNull(missedPeriods, "missedPeriods");
        Objects.requireNonNull(writeOffLimit, "writeOffLimit");
        Objects.requireNonNull(cancelled, "cancelled");
    }

    /**
     * Creates the seam of a handle that cannot be asked about cancellation.
     *
     * @param missedPeriods reader for the elapsed periods behind the deadline
     * @param writeOffLimit setter for the periods the next body may account for
     */
    public KairosTickControl(
        final ToLongFunction<SchedulableTick> missedPeriods,
        final ObjLongConsumer<SchedulableTick> writeOffLimit
    ) {
        this(missedPeriods, writeOffLimit, tick -> false);
    }

    /**
     * Returns the seam of a tick that cannot answer any of the questions.
     *
     * @return a control that reports no backlog, ignores the limit and is never cancelled
     */
    public static KairosTickControl none() {
        return new KairosTickControl(tick -> 0L, (tick, periods) -> {
            // the stock body writes off whatever it finds; nothing to limit
        }, tick -> false);
    }
}
