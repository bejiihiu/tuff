package kz.bejiihiu.tuff.threads.region;

import java.util.List;

/**
 * Read side of the region scheduler's tick data.
 *
 * <p>Kept behind an interface for one reason: the aggregation is the part worth testing, and it must be
 * testable without a server. The Folia implementation reads the live scheduler; a test hands in readings
 * and checks what the summary says about them.
 *
 * <p>An implementation is called from the sampler, at most once per second. It must not throw and must
 * not block on the regioniser from a region tick thread: an unreadable scheduler answers with an empty
 * list, which the summary renders as "no regions", not as a server error.
 */
@FunctionalInterface
public interface RegionTickProvider {

    /**
     * Reads every region the pool ticks.
     *
     * @return an immutable list of readings, in no particular order, possibly empty
     */
    List<RegionReading> read();
}
