/**
 * Unit tests of the metrics subsystem.
 *
 * <p>Every test drives the collector through its public hooks with a fake clock, so counters,
 * throughput and snapshots are deterministic and no test depends on the number of processors or on
 * wall clock timing.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.metrics;

import org.jspecify.annotations.NullMarked;
