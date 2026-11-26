/**
 * Tests for the two registries in {@code impl/pools}.
 *
 * <p>Cases that matter: the frozen canonical ids and their kinds, idempotent reload replacing a spec,
 * the three refusals (L0, kind change, registration after stop), immutable snapshots and the fact that
 * a snapshot taken while pools register is always internally consistent and never throws.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.pools;

import org.jspecify.annotations.NullMarked;
