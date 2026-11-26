package kz.bejiihiu.tuff.compute.internal.spi;

/**
 * The single source of the compute processor budget.
 *
 * <p>The number returned by {@link #processors()} is a <b>budget</b>, not the raw core count: it is
 * already net of whatever the platform reserved for region tick threads, chunk workers, IO threads
 * and headroom. The engine uses it for automatic worker sizing and stores it as the budget
 * {@link kz.bejiihiu.tuff.compute.AsyncComputeScheduler#applyProcessorBudget(int)} expects, so an
 * implementation must not leave a "subtract the reserves later" formula around: one process, one
 * source, one subtraction.
 *
 * <p>The default implementation is {@link #legacy()}: the JVM processor count, which is exactly what
 * the no-argument factory used before this port existed. A server that already computes a real
 * budget from live pool sizes wraps its formula behind {@link #fixed(int, String)} instead of
 * calling {@code applyProcessorBudget} with a hand-computed number.
 */
public interface ComputeBudgetPort {

    /**
     * Returns the processors the compute engine may use.
     *
     * @return the budget, always at least {@code 1}
     */
    int processors();

    /**
     * Returns a short label for the origin of the budget.
     *
     * <p>Diagnostics print it next to the worker count so an operator can tell a configured budget
     * from a legacy one. Keep it stable and short, for example {@code legacy}, {@code tuff.yml} or
     * {@code utmi}.
     *
     * @return the source label, never blank
     */
    String source();

    /**
     * Returns the default port: the JVM processor count, labelled {@code legacy}.
     *
     * @return the legacy port, never {@code null}
     */
    static ComputeBudgetPort legacy() {
        return LegacyComputeBudgetPort.INSTANCE;
    }

    /**
     * Returns a port for a budget the caller already resolved.
     *
     * @param processors processors the compute engine may use, at least {@code 1}
     * @param source     short label for the origin of the number, must not be blank
     * @return a fixed port
     * @throws IllegalArgumentException if {@code processors} is not positive or {@code source} is blank
     */
    static ComputeBudgetPort fixed(final int processors, final String source) {
        return new FixedComputeBudgetPort(processors, source);
    }
}
