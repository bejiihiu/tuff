/**
 * Tests for the budget ledger.
 *
 * <p>The numbers are asserted for four, eight, sixteen and thirty two cores because that is the range
 * the sizing guidance was written for, and because the small end is where the ledger runs out and the
 * degradation path has to fire instead of quietly overcommitting.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.budget;

import org.jspecify.annotations.NullMarked;
