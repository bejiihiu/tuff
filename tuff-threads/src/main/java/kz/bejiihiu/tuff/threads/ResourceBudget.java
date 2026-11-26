package kz.bejiihiu.tuff.threads;

/**
 * Declared CPU budget of a single pool.
 *
 * <p>The budget answers two questions a pool owner asks at bootstrap: "how many cores may I use" and
 * "how many of them are guaranteed to me even when everything else is busy". It is deliberately not a
 * promise about thread count: one core can run several threads that mostly block, and a thread can
 * consume less than a core. The resource controller translates budgets into leases and thread limits.
 *
 * <p>A budget is either fixed ({@link #fixed(int, int)}) or elastic ({@link #elastic(double, int)}).
 * A fixed budget names an absolute core count; an elastic budget names a fraction of the usable cores
 * and is resolved every time the usable capacity changes. {@link #reservedCores()} is the floor the
 * controller may not take away for other pools: it survives adaptive shrinking and pressure from
 * non critical work. Reserves across all pools are expected to fit into the usable capacity; the
 * controller reports a degraded state instead of silently overcommitting when they do not.
 *
 * @param cores         absolute core cap, or {@link #ELASTIC} when the budget is expressed as a share
 * @param reservedCores guaranteed floor, may be zero but never exceeds a fixed {@link #cores}
 * @param share         fraction of the usable cores used when {@link #cores} is {@link #ELASTIC},
 *                      greater than zero and at most one
 */
public record ResourceBudget(int cores, int reservedCores, double share) {

    /**
     * Sentinel for "size this budget from {@link #share()}" instead of an absolute core count.
     */
    public static final int ELASTIC = -1;

    /**
     * Validates a budget.
     *
     * @throws IllegalArgumentException if the core counts or the share are inconsistent
     */
    public ResourceBudget {
        if (cores < ELASTIC) {
            throw new IllegalArgumentException("cores must be -1 or at least 0, got " + cores);
        }
        if (reservedCores < 0) {
            throw new IllegalArgumentException("reservedCores must not be negative, got " + reservedCores);
        }
        if (cores >= 0 && reservedCores > cores) {
            throw new IllegalArgumentException(
                "reservedCores must not exceed a fixed core cap, got " + reservedCores + " > " + cores
            );
        }
        if (!(share > 0.0d) || share > 1.0d) {
            throw new IllegalArgumentException("share must be in (0, 1], got " + share);
        }
    }

    /**
     * Creates a budget with an absolute core cap.
     *
     * @param cores         core cap, at least zero
     * @param reservedCores guaranteed floor, between zero and {@code cores}
     * @return the budget
     * @throws IllegalArgumentException if the arguments are inconsistent
     */
    public static ResourceBudget fixed(final int cores, final int reservedCores) {
        return new ResourceBudget(cores, reservedCores, 1.0d);
    }

    /**
     * Creates a budget expressed as a share of the usable cores.
     *
     * @param share         fraction of usable cores, greater than zero and at most one
     * @param reservedCores guaranteed floor, at least zero
     * @return the budget
     * @throws IllegalArgumentException if the arguments are inconsistent
     */
    public static ResourceBudget elastic(final double share, final int reservedCores) {
        return new ResourceBudget(ELASTIC, reservedCores, share);
    }

    /**
     * Returns the default budget of a pool that may use everything that is left.
     *
     * @return an elastic budget with share {@code 1.0} and no reserve
     */
    public static ResourceBudget defaults() {
        return elastic(1.0d, 0);
    }

    /**
     * Checks whether the budget is derived from a share instead of an absolute cap.
     *
     * @return {@code true} if {@link #cores()} is {@link #ELASTIC}
     */
    public boolean isElastic() {
        return this.cores == ELASTIC;
    }

    /**
     * Resolves the budget against the currently usable core count.
     *
     * <p>A fixed budget is clamped to the usable capacity: a pool can never use more cores than the
     * machine has. An elastic budget is rounded down and floored at one core, because a registered pool
     * with zero cores could never make progress. The reserve is not subtracted here; it is a floor
     * applied by the controller when it divides capacity between pools.
     *
     * @param usableCores cores available to all managed pools after the reserve, at least one
     * @return the number of cores this budget resolves to, between zero and {@code usableCores};
     *     only an elastic budget is guaranteed to resolve to at least one
     * @throws IllegalArgumentException if {@code usableCores} is below one
     */
    public int resolveCores(final int usableCores) {
        if (usableCores < 1) {
            throw new IllegalArgumentException("usableCores must be at least 1, got " + usableCores);
        }
        if (!this.isElastic()) {
            return Math.min(this.cores, usableCores);
        }
        return Math.max(1, (int) Math.floor(usableCores * this.share));
    }
}
