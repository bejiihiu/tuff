/**
 * Tests of the region relief path.
 *
 * <p>The tests drive the real engine with a frozen clock and a single threaded executor standing in
 * for a region thread, so worker counts and timing are independent of the host.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.relief.impl;

import org.jspecify.annotations.NullMarked;
