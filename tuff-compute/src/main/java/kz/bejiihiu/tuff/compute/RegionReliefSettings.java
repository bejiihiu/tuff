package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration of the region relief path.
 *
 * <p>Relief is a separate concern from the generic consumer tree: it has its own switch, its own
 * in-flight bound and a core reserve that tick pressure must not take away. It is carried outside
 * {@link ComputeSettings} so an embedder that does not know about relief keeps compiling and keeps
 * exactly the old behaviour.
 *
 * <p>The defaults are deliberately quiet. {@link #enabled()} is {@code false}: the consumer and the
 * {@link RegionRelief} service exist and are usable, but a submission runs its work inline instead of
 * queueing it, which is what a server that never opted in already does. Switching it on is what turns
 * offloading on.
 *
 * <p>{@link #reservedCores()} only takes effect while {@link #enabled()} is {@code true}, so a server
 * that never uses relief also never withholds a core from tick pressure. The consumer policy defaults
 * match the shape relief needs: a bounded queue, refusal instead of eviction, and a short queue wait
 * because a stale relief result is worthless.
 *
 * @param enabled              whether relief work may be offloaded at all
 * @param maxInFlightPerRegion accepted requests one region may have outstanding at the same time
 * @param reservedCores        cores the compute pool keeps for relief under tick pressure, at least
 *                             zero, only applied while {@link #enabled()} is {@code true}
 * @param consumerPolicy       policy of the {@code region-relief} consumer
 */
public record RegionReliefSettings(
    boolean enabled,
    int maxInFlightPerRegion,
    int reservedCores,
    ConsumerPolicy consumerPolicy
) {

    /**
     * Id of the consumer that receives every offloaded relief request.
     */
    public static final String CONSUMER_ID = "region-relief";

    /**
     * Validates the settings.
     *
     * @throws NullPointerException     if {@code consumerPolicy} is {@code null}
     * @throws IllegalArgumentException if the in-flight cap is below one or the reserve is negative
     */
    public RegionReliefSettings {
        Objects.requireNonNull(consumerPolicy, "consumerPolicy");
        if (maxInFlightPerRegion < 1) {
            throw new IllegalArgumentException(
                "maxInFlightPerRegion must be at least 1, got " + maxInFlightPerRegion
            );
        }
        if (reservedCores < 0) {
            throw new IllegalArgumentException("reservedCores must not be negative, got " + reservedCores);
        }
    }

    /**
     * Returns the defaults: offloading off, eight requests per region, one reserved core and the
     * dedicated consumer policy.
     *
     * @return the default relief settings
     */
    public static RegionReliefSettings defaults() {
        return new RegionReliefSettings(false, 8, 1, defaultConsumerPolicy());
    }

    /**
     * Returns the consumer policy relief uses when the configuration does not override it.
     *
     * <p>A quarter of the execution capacity, at most sixty four queued requests, refusal instead of
     * eviction and a two second queue wait: relief work is only useful if it is fresh, so a request
     * that waited longer runs inline instead of arriving late.
     *
     * @return the default consumer policy
     */
    public static ConsumerPolicy defaultConsumerPolicy() {
        return new ConsumerPolicy(
            true,
            ConsumerPolicy.ConcurrencyLimit.relative(0.25d),
            64,
            0.5d,
            OverflowPolicy.REJECT,
            Duration.ofSeconds(2L)
        );
    }

    /**
     * Returns the reserve that is actually held back from tick pressure.
     *
     * <p>Zero while relief is off, so a server that never opted in hands every core to the pressure
     * policy exactly as before.
     *
     * @return the effective reserve in cores, never negative
     */
    public int effectiveReservedCores() {
        return this.enabled ? this.reservedCores : 0;
    }

    /**
     * Returns a copy with a different consumer policy.
     *
     * @param policy replacement policy
     * @return the new settings
     * @throws NullPointerException if {@code policy} is {@code null}
     */
    public RegionReliefSettings withConsumerPolicy(final ConsumerPolicy policy) {
        return new RegionReliefSettings(this.enabled, this.maxInFlightPerRegion, this.reservedCores, policy);
    }

    /**
     * Policy of the per-region overrun trigger.
     *
     * <p>The trigger is the decision "this region is behind, ask the registered work units for help".
     * It watches the scheduler independent tick feed, so it does not care which scheduler ran the body,
     * and it keeps independent counters per region.
     *
     * <p>Two kinds of limits keep it from thrashing. Hysteresis: a window opens after
     * {@link #enterOverruns()} consecutive overrun bodies and closes after {@link #exitCalm()}
     * consecutive calm ones. Rate: a closed window stays closed for {@link #cooldown()}, and one region
     * may open at most {@link #maxWindows()} windows inside {@link #rateWindow()}.
     *
     * <p>The defaults are quiet: the trigger is off, and switching it on does nothing until a subsystem
     * registers a work unit. See the region relief documentation for the exact predicate.
     *
     * @param enabled       whether overrun bodies may open a relief window at all
     * @param overrun       body duration, or start lag, that counts as an overrun
     * @param enterOverruns consecutive overrun bodies that open a window
     * @param exitCalm      consecutive calm bodies that close an open window
     * @param cooldown      time a region must stay calm after a window closed before the next one
     * @param maxWindows    windows one region may open inside one rate window
     * @param rateWindow    length of the sliding rate window of one region
     * @param staleAfter    silence after which a region's trigger state is forgotten
     */
    public record Trigger(
        boolean enabled,
        Duration overrun,
        int enterOverruns,
        int exitCalm,
        Duration cooldown,
        int maxWindows,
        Duration rateWindow,
        Duration staleAfter
    ) {

        /**
         * Validates the policy.
         *
         * @throws NullPointerException     if a duration is {@code null}
         * @throws IllegalArgumentException if a duration is not usable or a count is below one
         */
        public Trigger {
            Objects.requireNonNull(overrun, "overrun");
            Objects.requireNonNull(cooldown, "cooldown");
            Objects.requireNonNull(rateWindow, "rateWindow");
            Objects.requireNonNull(staleAfter, "staleAfter");
            if (overrun.isZero() || overrun.isNegative()) {
                throw new IllegalArgumentException("overrun must be positive, got " + overrun);
            }
            if (enterOverruns < 1) {
                throw new IllegalArgumentException("enterOverruns must be at least 1, got " + enterOverruns);
            }
            if (exitCalm < 1) {
                throw new IllegalArgumentException("exitCalm must be at least 1, got " + exitCalm);
            }
            if (cooldown.isNegative()) {
                throw new IllegalArgumentException("cooldown must not be negative, got " + cooldown);
            }
            if (maxWindows < 1) {
                throw new IllegalArgumentException("maxWindows must be at least 1, got " + maxWindows);
            }
            if (rateWindow.isZero() || rateWindow.isNegative()) {
                throw new IllegalArgumentException("rateWindow must be positive, got " + rateWindow);
            }
            if (staleAfter.isZero() || staleAfter.isNegative()) {
                throw new IllegalArgumentException("staleAfter must be positive, got " + staleAfter);
            }
        }

        /**
         * Returns the defaults: the trigger is off, so a server that never opted in pays one flag check
         * per tick and nothing else.
         *
         * @return the default trigger policy
         */
        public static Trigger defaults() {
            return new Trigger(
                false,
                Duration.ofMillis(40L),
                3,
                6,
                Duration.ofSeconds(30L),
                4,
                Duration.ofMinutes(5L),
                Duration.ofSeconds(10L)
            );
        }
    }
}
