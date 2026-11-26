package kz.bejiihiu.tuff.threads.internal.impl.budget;

/**
 * Threads already claimed before the pools are budgeted.
 *
 * <p>These are the lanes that exist outside the managed budget: region tick threads, chunk workers and
 * chunk IO threads. They are subtracted from the headroom before anything else is divided, which is the
 * only honest order: their threads are already running when the first lease is handed out.
 *
 * <p>Counts are threads, not cores, matching how the server configures them. A blocking IO thread does
 * not consume a core, so subtracting it is conservative on purpose.
 *
 * @param regionThreads tick threads of the region scheduler, not negative
 * @param chunkThreads  chunk system worker threads, not negative
 * @param ioThreads     chunk system IO threads, not negative
 */
public record BudgetDeductions(int regionThreads, int chunkThreads, int ioThreads) {

    /**
     * Validates the deduction set.
     *
     * @throws IllegalArgumentException if a count is negative
     */
    public BudgetDeductions {
        if (regionThreads < 0) {
            throw new IllegalArgumentException("regionThreads must not be negative, got " + regionThreads);
        }
        if (chunkThreads < 0) {
            throw new IllegalArgumentException("chunkThreads must not be negative, got " + chunkThreads);
        }
        if (ioThreads < 0) {
            throw new IllegalArgumentException("ioThreads must not be negative, got " + ioThreads);
        }
    }

    /**
     * Returns a deduction set that claims nothing.
     *
     * <p>Used before the region scheduler is up and in tests that only care about the ratio arithmetic.
     *
     * @return zero deductions
     */
    public static BudgetDeductions none() {
        return new BudgetDeductions(0, 0, 0);
    }
}
