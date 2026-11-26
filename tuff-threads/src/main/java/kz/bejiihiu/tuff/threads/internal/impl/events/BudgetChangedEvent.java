package kz.bejiihiu.tuff.threads.internal.impl.events;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A pool's declared CPU budget changed.
 *
 * <p>Budgets move on reload and when the controller re-divides capacity; both look identical from the
 * outside unless the transition is recorded. The previous values are carried so a recording does not
 * need the neighbouring event to explain a jump.
 */
@Name("tuff.threads.BudgetChanged")
@Label("Pool Budget Changed")
@Description("The declared CPU budget of a pool changed")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class BudgetChangedEvent extends Event {

    /**
     * Pool whose budget moved.
     */
    @Label("Pool")
    public String poolId;

    /**
     * Previous absolute core cap, or {@code -1} when it was elastic.
     */
    @Label("Previous Cores")
    public int previousCores;

    /**
     * New absolute core cap, or {@code -1} when it is elastic.
     */
    @Label("Cores")
    public int cores;

    /**
     * Previous guaranteed floor.
     */
    @Label("Previous Reserved Cores")
    public int previousReservedCores;

    /**
     * New guaranteed floor.
     */
    @Label("Reserved Cores")
    public int reservedCores;

    /**
     * Previous share of the usable cores.
     */
    @Label("Previous Share")
    public double previousShare;

    /**
     * New share of the usable cores.
     */
    @Label("Share")
    public double share;
}
