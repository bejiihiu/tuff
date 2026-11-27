package kz.bejiihiu.tuff.threads.pressure;

import kz.bejiihiu.tuff.threads.ResourceSnapshot;

/**
 * Receives every pass the thread sampler publishes.
 *
 * <p>The sampler owns the one thread that reads regions, probes and the thread table, so anything that
 * wants those numbers reacts to a pass instead of starting a timer of its own. A listener runs on the
 * sampler thread before the next sleep and must be quick and non throwing: it is on the path that keeps
 * {@code /tuff threads} fresh.
 */
@FunctionalInterface
public interface TickSampleListener {

    /**
     * Handles one published pass.
     *
     * @param snapshot     the snapshot that was just published
     * @param takenAtNanos monotonic timestamp of the pass
     */
    void onSample(ResourceSnapshot snapshot, long takenAtNanos);
}
