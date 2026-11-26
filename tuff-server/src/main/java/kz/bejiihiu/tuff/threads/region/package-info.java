/**
 * Read side of the region scheduler: per-region tick readings and their aggregate.
 *
 * <p>Split on purpose. {@link kz.bejiihiu.tuff.threads.region.RegionTickProvider} is the seam that knows
 * Folia, {@link kz.bejiihiu.tuff.threads.region.RegionPoolMetricsAggregator} is the arithmetic and knows
 * nothing else. The aggregate is the part an operator reads, so it is the part covered by tests without a
 * server.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.region;

import org.jspecify.annotations.NullMarked;
