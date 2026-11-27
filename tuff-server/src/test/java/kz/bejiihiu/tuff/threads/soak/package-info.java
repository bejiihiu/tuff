/**
 * Soak and fault-injection tests for the region scheduler.
 *
 * <p>These tests drive the real scheduler through its public seams and inject the failures the rollout
 * plan names: a dead supervisor, a refused allocation, a halt during a spawn or a retire, a repeated
 * halt, a churn storm of schedule and cancel, and a profiler attached to every tick. They assert the
 * shutdown and accounting invariants with observations rather than with timing assumptions, and every
 * wait is bounded.
 *
 * <p>The package is deliberately separate from the scheduler's own unit tests: it is the layer that
 * treats the scheduler as a black box, so a refactor inside the scheduler cannot silently remove the
 * evidence that the invariants still hold.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.soak;

import org.jspecify.annotations.NullMarked;
