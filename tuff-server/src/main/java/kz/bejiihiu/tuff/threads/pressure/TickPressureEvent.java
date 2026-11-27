package kz.bejiihiu.tuff.threads.pressure;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * The compute pool yielded a core to lagging regions or took one back.
 *
 * <p>Emitted only when the reserve moves, never once per pass: a recording then contains the edges an
 * operator has to line up with a latency graph, and nothing else. The fields carry the whole decision -
 * the reason, the reserve, the budget and the signal it was made from - so a recording explains itself
 * without the log.
 */
@Name("tuff.threads.TickPressure")
@Label("Tick Pressure")
@Description("The compute pool changed how many cores it yields to lagging regions")
@Category({"Tuff", "Threads"})
@StackTrace(false)
public final class TickPressureEvent extends Event {

    /**
     * {@code NORMAL} or {@code PRESSURE}.
     */
    @Label("State")
    public String state;

    /**
     * Why the reserve moved: {@code LAG_ENTER}, {@code LAG_STEP_UP}, {@code CALM_STEP_DOWN},
     * {@code CALM_EXIT} or {@code CEILING_REACHED}.
     */
    @Label("Reason")
    public String reason;

    /**
     * Cores the compute pool is yielding after the change.
     */
    @Label("Reserve Cores")
    public int reserveCores;

    /**
     * Most cores it could yield on this machine.
     */
    @Label("Max Reserve Cores")
    public int maxReserveCores;

    /**
     * Processor budget the pool should run at.
     */
    @Label("Budget")
    public int budget;

    /**
     * Budget ledger's usable cores at the time of the decision.
     */
    @Label("Usable Cores")
    public int usableCores;

    /**
     * Regions the region pool ticked.
     */
    @Label("Regions")
    public int regions;

    /**
     * Share of regions below 18 TPS.
     */
    @Label("Lag Fraction")
    public double lagFraction;

    /**
     * Slowest region's TPS, or {@code -1} when no region reported.
     */
    @Label("Minimum TPS")
    public double minimumTps;

    /**
     * Worst single tick across regions, in milliseconds.
     */
    @Label("Maximum MSPT (ms)")
    public double maximumMspt;

    /**
     * Emits the event when a recording has it enabled.
     *
     * <p>The object is built before the check on purpose: it is one allocation per reserve change
     * (at most one per cooldown), and {@code Event#isEnabled()} has to be asked on the instance.
     *
     * @param snapshot the decision to record
     */
    static void emit(final TickPressureSnapshot snapshot) {
        try {
            final TickPressureEvent event = new TickPressureEvent();
            if (!event.isEnabled()) {
                return;
            }
            event.state = snapshot.state().name();
            event.reason = snapshot.reason().name();
            event.reserveCores = snapshot.reserve();
            event.maxReserveCores = snapshot.maxReserve();
            event.budget = snapshot.budget();
            event.usableCores = snapshot.usableCores();
            event.regions = snapshot.signal().regions();
            event.lagFraction = snapshot.signal().lagFraction();
            event.minimumTps = snapshot.signal().minimumTps();
            event.maximumMspt = snapshot.signal().maximumMspt();
            event.commit();
        } catch (Throwable _) {
            // profiling must never break the sampler
        }
    }
}
