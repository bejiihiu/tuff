/**
 * Sampling, snapshots and pool accounting of the unified thread manager.
 *
 * <p>Nothing here has a thread of its own. The sampler runs when somebody asks for a snapshot, at most
 * once per configured interval, and the pass is one walk over registered threads plus one probe call per
 * pool. That is the whole overhead model: no background ticker, no worker can be blocked by metrics, and
 * a server that never asks costs nothing.
 *
 * <p>The pass is bounded and single flight. Two commands asking at the same time get one pass and one
 * snapshot; the loser of the lock reads the previous snapshot instead of waiting, so a command handler
 * can never be delayed by a slow probe.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import org.jspecify.annotations.NullMarked;
