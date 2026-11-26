/**
 * Observation of pools that the thread layer does not own.
 *
 * <p>A {@link kz.bejiihiu.tuff.threads.internal.spi.PoolProbe} is the only integration an observe-only
 * pool needs: no factory, no budget, no lifecycle handover, just counters and the thread list. The first
 * probe here covers Folia's region tick pool, whose scheduler, thread class and scheduling stay untouched
 * - this package reads them and nothing else.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.probe;

import org.jspecify.annotations.NullMarked;
