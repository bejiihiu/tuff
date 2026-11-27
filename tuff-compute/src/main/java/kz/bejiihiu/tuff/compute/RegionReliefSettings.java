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
}
