/**
 * Scenario tests for the interactions between the UTMI features.
 *
 * <p>Every test here drives the real controller, the real tick pressure monitor and the real compute
 * adapter, but nothing starts a server: regions are a hand-built snapshot, the clock is a number the
 * test moves, the engine is a recorder and the pool set is an in-memory registry. That is enough to
 * check the seams between the features - pressure lowering the budget, shedding lowering capacity,
 * and both of them showing up in one snapshot - which the per-feature unit tests cannot see.
 *
 * <p>No test advances real time: there is no sleep in the scenario clock and every "sixty seconds" is
 * sixty calls with a timestamp of the test's choosing.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.scenario;

import org.jspecify.annotations.NullMarked;
