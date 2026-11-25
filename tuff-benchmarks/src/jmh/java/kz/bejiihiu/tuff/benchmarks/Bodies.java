package kz.bejiihiu.tuff.benchmarks;

import kz.bejiihiu.tuff.compute.ComputeTask;

/**
 * Task body with a known cost, selected by a constant.
 *
 * <p>Non capturing, so the caller can keep one instance and submit it many times.
 */
final class Bodies {

    /** Empty body: measures the scheduler around it, not the work. */
    static final ComputeTask<Boolean> NOOP = Fixtures.NOOP;

    /** ~0.5-0.7 us of dependent integer work, on the order of a tiny game-logic query. */
    static final ComputeTask<Long> SHORT = Fixtures.SHORT;

    /** ~4-6 us of dependent integer work, on the order of a small chunk section. */
    static final ComputeTask<Long> CPU = () -> Fixtures.mix(4096);

    private Bodies() {
    }
}
