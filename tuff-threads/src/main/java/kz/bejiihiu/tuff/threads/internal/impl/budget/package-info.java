/**
 * The single budget arithmetic of the manager.
 *
 * <p>Every core count the pools see is produced here and nowhere else. The old arrangement had the
 * compute engine subtract region, chunk, io and network reserves from the processor count on its own
 * while the rest of the server sized itself from a different formula, which is how the same core got
 * promised twice. This package owns the ledger; the controller owns the leases on top of it.
 *
 * <p>A plan is computed once per manager. There is deliberately no {@code refresh}: recomputing against
 * a topology that a live process can no longer change only produces two ledgers that disagree, and the
 * one an operator reads would be the wrong one.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.budget;

import org.jspecify.annotations.NullMarked;
