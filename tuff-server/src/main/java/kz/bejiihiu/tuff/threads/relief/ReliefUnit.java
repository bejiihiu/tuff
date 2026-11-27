package kz.bejiihiu.tuff.threads.relief;

/**
 * A subsystem that has work it can shed while its region is behind.
 *
 * <p>A unit is the whole integration point between the trigger and the work: it knows its location,
 * its scheduler and what a safe snapshot is, so it builds its own
 * {@link kz.bejiihiu.tuff.compute.RegionReliefOwner} and submits its own
 * {@link kz.bejiihiu.tuff.compute.RegionReliefWork}. The trigger never creates work, never submits and
 * never applies anything.
 *
 * <p>Both callbacks run on the owning region thread that produced the tick, during the tick body that
 * ended, so a callback must be short: capture state, submit, return. A unit that throws is isolated
 * and counted; the tick and the other units continue.
 *
 * <p>Register one instance per subsystem and keep it for the lifetime of the server. Registration is
 * by instance identity.
 */
public interface ReliefUnit {

    /**
     * Returns the stable id of the unit, used in logs and in the command output.
     *
     * @return a short id, never {@code null}
     */
    String id();

    /**
     * Called on the region thread when a relief window opens for that region.
     *
     * <p>This is the moment to capture a snapshot and submit it. A unit that has nothing to shed for
     * this region should return immediately.
     *
     * @param window the window that opened
     */
    void onWindowOpen(ReliefWindow window);

    /**
     * Called on the region thread when the window closes because the region became calm again.
     *
     * <p>Work already submitted stays valid until its own epoch check says otherwise; this callback
     * exists so a unit can stop submitting and release per-window state. It is not called when a
     * region disappears without ticking again, because there is no region thread left to run it on.
     *
     * @param window the window that closed
     */
    default void onWindowClose(final ReliefWindow window) {
    }
}
