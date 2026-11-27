package kz.bejiihiu.tuff.threads.relief;

import java.util.Objects;

/**
 * One relief window of one region.
 *
 * <p>It is handed to a work unit on the region thread the moment the window opens. The region id and
 * the owning thread say where the work must be applied, the epoch says which window it belongs to, and
 * the open timestamp lets a unit bound how old the snapshot may be.
 *
 * <p>The epoch is monotonic per region incarnation: it starts at one and every later window of the
 * same region gets a higher number. A unit can keep the epoch it was called with and check it later,
 * but the trigger itself does not decide whether a result is stale.
 *
 * @param regionId      id of the region that is behind
 * @param owner         thread that runs the region; the unit may only submit from it
 * @param epoch         window epoch of this region, at least one
 * @param openedAtNanos moment the window opened, in {@link System#nanoTime()} terms
 */
public record ReliefWindow(long regionId, Thread owner, long epoch, long openedAtNanos) {

    /**
     * Validates the window.
     *
     * @throws NullPointerException     if {@code owner} is {@code null}
     * @throws IllegalArgumentException if the epoch is below one
     */
    public ReliefWindow {
        Objects.requireNonNull(owner, "owner");
        if (epoch < 1L) {
            throw new IllegalArgumentException("epoch must be positive, got " + epoch);
        }
    }
}
