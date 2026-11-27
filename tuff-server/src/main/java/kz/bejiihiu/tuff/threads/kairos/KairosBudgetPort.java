package kz.bejiihiu.tuff.threads.kairos;

/**
 * Where the fund asks how many tick threads the machine can honestly carry.
 *
 * <p>The port exists so Kairos never keeps a second copy of the processor arithmetic. When the thread
 * layer runs, the answer comes from its resource controller, which already subtracted the deducted
 * lanes and the reserve; when it does not, the caller passes nothing and the scheduler falls back to
 * the documented {@code floor(cores * 0.8)} rule. A number the controller already produced must not be
 * reduced by the reserve again - that would subtract the same core twice.
 *
 * <p>The ceiling is resolved once, when the scheduler starts. A budget change therefore needs a
 * restart, exactly like the rest of the Kairos section.
 */
@FunctionalInterface
public interface KairosBudgetPort {

    /**
     * Returns the most tick threads the fund may have.
     *
     * <p>The answer is the whole region lane, not the headroom left after the floor: the fund's floor
     * is part of it. A value below {@code floor} does not fail the server; the scheduler pins the fund
     * at the floor and reports {@code NO_HEADROOM}.
     *
     * @param floor current floor of the fund, at least one
     * @return the ceiling to use, which may be below {@code floor}
     */
    int ceiling(int floor);

    /**
     * Describes the source of the ceiling for the command and the startup log.
     *
     * @return a short human readable description, never {@code null}
     */
    default String describe() {
        return "thread layer budget";
    }
}
