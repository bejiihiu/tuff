package kz.bejiihiu.tuff.threads.idle;

import java.util.function.BooleanSupplier;

/**
 * One unit of housekeeping that may run on a tick thread while that thread has nothing to tick.
 *
 * <p>Implementing this interface is the whole permission: the runtime never invokes a plain
 * {@link Runnable}, a future or a compute task, so work that was not written for this path cannot
 * reach it. The promises in the package documentation are part of the contract, not advice - a unit
 * that blocks, does I/O or touches a region's world state is a defect even if it is short.
 *
 * <p>Cooperation: the caller supplies {@code canContinue}, which is the slice deadline and the tick
 * cancellation signal in one answer. A unit that can run for more than a few microseconds must call
 * it between pieces of work and return as soon as it answers {@code false}; nothing interrupts the
 * thread on the unit's behalf. Returning early is normal: a source may offer the same unit again
 * later, and the runtime does not expect a unit to finish in one slice.
 *
 * <p>Execution: on a platform tick thread, with no lock held by the runtime, never while the region
 * that thread owns is inside its tick body. Exceptions are caught by the caller so a broken unit
 * cannot take the fund down.
 */
@FunctionalInterface
public interface IdleWork {

    /**
     * Runs one bounded slice of the unit.
     *
     * @param canContinue answer to "may I keep going", false after the slice deadline or when a tick
     *                    needs the thread; must be checked between pieces of work
     */
    void run(BooleanSupplier canContinue);
}
