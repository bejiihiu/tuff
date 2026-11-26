package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.util.List;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;

/**
 * Read side of the attached pool probes.
 *
 * <p>The frozen {@code PoolProbeRegistry} contract only has {@code attach} and {@code detach}; nothing in
 * the SPI can enumerate what is attached. The metrics layer therefore takes this narrow supplier, which
 * the runtime implements over the probe registry it already owns. Keeping it a supplier, not a copy, is
 * what makes a late {@code attach} visible to the next sampling pass without rewiring anything.
 *
 * <p>Called once per pass from the sampler thread. An implementation must be cheap and must not throw.
 */
@FunctionalInterface
public interface PoolProbeSource {

    /**
     * Returns the probes attached right now.
     *
     * @return an immutable snapshot of attached probes, possibly empty
     */
    List<PoolProbe> probes();

    /**
     * Returns a source with no probes, for a manager that only observes registered threads.
     *
     * @return the empty source
     */
    static PoolProbeSource empty() {
        return List::of;
    }
}
