/**
 * Tests for the scheduling queue and the quota controller.
 *
 * <p>Every test drives the real implementation through its {@code ComputeQueue} or
 * {@code QuotaController} contract. Only foreign components are substituted: consumers are plain
 * strings, handles are minimal value objects and the clock is driven by hand where aging has to be
 * exact.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.queue.impl;

import org.jspecify.annotations.NullMarked;
