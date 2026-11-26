/**
 * End-to-end tests of the execution lanes: virtual-thread routing, spill, quotas across both lanes,
 * cancellation, shutdown, metrics, labels, adaptive interaction and a mixed-traffic soak.
 *
 * <p>Everything here is driven through the public {@code AsyncComputeScheduler} built by
 * {@code ComputeSchedulers}, so the real queue, dispatcher, pool, janitor and virtual lane all take
 * part. Only the clock is replaced, because it is the documented way to age, expire and rescale
 * without sleeping.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.lanes;

import org.jspecify.annotations.NullMarked;
