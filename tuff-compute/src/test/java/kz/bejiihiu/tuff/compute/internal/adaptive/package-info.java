/**
 * Tests for the adaptive concurrency controller.
 *
 * <p>Every test drives the real controller through its contract with a fake clock, so a decision
 * window is exactly one call to {@code advance}; nothing sleeps and no test depends on wall clock
 * speed.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.adaptive;

import org.jspecify.annotations.NullMarked;
