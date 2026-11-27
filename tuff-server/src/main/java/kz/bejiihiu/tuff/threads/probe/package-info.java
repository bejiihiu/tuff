/**
 * Observation of pools that the thread layer does not own.
 *
 * <p>A {@link kz.bejiihiu.tuff.threads.internal.spi.PoolProbe} is the only integration an observe-only
 * pool needs: no factory, no budget, no lifecycle handover, just counters and the thread list. The
 * probes here cover Folia's region tick pool, the Moonrise chunk worker and IO pools, the netty event
 * loops, the chat executor and the background worker pool. Each one reads its owner and changes
 * nothing: no resize, no park, no shutdown.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.probe;

import org.jspecify.annotations.NullMarked;
