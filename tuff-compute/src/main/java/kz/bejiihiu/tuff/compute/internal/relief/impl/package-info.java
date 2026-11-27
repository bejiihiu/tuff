/**
 * Region relief: the offload path an overloaded region uses to hand pure computation to the pool.
 *
 * <p>The implementation owns no queue and no thread. It enforces the rules the scheduler cannot know -
 * submit on the owner thread, a bounded number of requests per owner, apply only while the snapshot
 * epoch is current, never apply off the owner thread - and delegates everything else to the
 * {@code region-relief} consumer of the engine.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.relief.impl;

import org.jspecify.annotations.NullMarked;
