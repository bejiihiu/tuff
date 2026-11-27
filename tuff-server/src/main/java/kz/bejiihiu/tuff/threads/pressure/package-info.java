/**
 * Compute pool pressure control driven by region tick health.
 *
 * <p>When regions fall behind, the compute pool is a large pool of background workers on the same
 * machine, so giving it a few cores back is the cheapest way to stop the bleeding. This package
 * decides how many cores to yield and when to take them back, using the region summary the thread
 * sampler already measures - never a tick report read from a region thread.
 *
 * <p>The feature is off by default. It is a policy, not a scheduler: it only changes the processor
 * budget of the compute pool through the adapter, and it never touches Folia's tick threads, order or
 * ownership. With the pool unmanaged the decisions are still logged and reported, but nothing is
 * applied.
 *
 * <p>The split is deliberate: {@link kz.bejiihiu.tuff.threads.pressure.TickPressureController} is a
 * pure state machine with hysteresis, a deadband, a cooldown and a ceiling, and
 * {@link kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor} is the thin glue that feeds it from the
 * sampler and pushes the reserve to the one budget sink.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.pressure;

import org.jspecify.annotations.NullMarked;
