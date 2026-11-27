package kz.bejiihiu.tuff.threads.kairos;

import ca.spottedleaf.concurrentutil.scheduler.SchedulableTick;
import java.util.Objects;
import java.util.function.ObjLongConsumer;
import java.util.function.ToLongFunction;

/**
 * The two questions the fund has to ask a region tick handle, and nothing else.
 *
 * <p>A {@link SchedulableTick} only promises a deadline and a tick body. Catch-up needs two facts that
 * live inside Folia's handle next to its private {@code ca.spottedleaf.common.time.Schedule}: how many
 * tick periods the handle is behind right now, and how many of them the next body may write off. The
 * handle exposes exactly those two, and this record carries them into the package without teaching the
 * fund about regions.
 *
 * <p>{@link #none()} is the answer for a tick that is not a region handle - a test double, or a
 * scheduler used outside Folia. It reports no backlog and accepts the write-off limit without doing
 * anything, so catch-up quietly does not engage and the fund keeps the stock behaviour.
 *
 * <p>Both calls happen on the runner thread that holds the permit for the tick, just before a body
 * runs, and both must be cheap and must never block.
 *
 * @param missedPeriods reader for the elapsed periods behind the deadline
 * @param writeOffLimit setter for the periods the next body may account for
 */
public record KairosTickControl(
    ToLongFunction<SchedulableTick> missedPeriods,
    ObjLongConsumer<SchedulableTick> writeOffLimit
) {

    /**
     * Validates the seam.
     *
     * @throws NullPointerException if a function is {@code null}
     */
    public KairosTickControl {
        Objects.requireNonNull(missedPeriods, "missedPeriods");
        Objects.requireNonNull(writeOffLimit, "writeOffLimit");
    }

    /**
     * Returns the seam of a tick that cannot answer either question.
     *
     * @return a control that reports no backlog and ignores the limit, never {@code null}
     */
    public static KairosTickControl none() {
        return new KairosTickControl(tick -> 0L, (tick, periods) -> {
            // the stock body writes off whatever it finds; nothing to limit
        });
    }
}
