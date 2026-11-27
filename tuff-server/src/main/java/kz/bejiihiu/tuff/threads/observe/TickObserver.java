package kz.bejiihiu.tuff.threads.observe;

/**
 * Receives one call per executed region tick body, whatever scheduler ran it.
 *
 * <p>The tick body calls this after it has measured its own end, with the deadline it was actually
 * measured against. The deadline is the value the handle held before it advanced the schedule, so a
 * receiver never has to reconstruct it from the periods the body wrote off.
 *
 * <p>Implementations run on the tick thread. They must be allocation free when nothing is recorded and
 * must never throw: an observation is diagnostics, not part of the tick contract.
 */
@FunctionalInterface
public interface TickObserver {

    /**
     * Observes one executed tick body.
     *
     * @param origin               scheduler that ran the body, never {@code null}
     * @param regionId             region of the body, or {@code -1} for a tick that has no region
     * @param tickId               id of the scheduled tick
     * @param scheduledStartNanos  deadline the body was measured against, before the schedule advanced
     * @param startNanos           moment the body started
     * @param endNanos             moment the body returned
     * @param periodsAhead         whole periods the handle was behind at the body start; one is on time
     * @param tickCount            periods the handle accounted for in this body; one means nothing was written off
     */
    void onTick(
        TickOrigin origin,
        long regionId,
        long tickId,
        long scheduledStartNanos,
        long startNanos,
        long endNanos,
        long periodsAhead,
        long tickCount
    );
}
