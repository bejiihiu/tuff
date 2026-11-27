package kz.bejiihiu.tuff.threads;

import java.util.List;
import java.util.Objects;

/**
 * How much of the budget is held for the critical lanes, and how much of it is withheld from the rest.
 *
 * <p>Two numbers that look similar and are not:
 * <ul>
 *     <li>{@link #criticalReserveCores()} is permanent for the life of the ledger. It is the part of the
 *     budget the non critical pools may never take, even when they oversubscribe their own caps, because
 *     region ticks and network event loops are what the whole server depends on.</li>
 *     <li>{@link #shedCores()} is temporary. The policy takes it out of the non critical pools one step at
 *     a time while a critical pool's lease queue grows, and gives it back one step at a time after the
 *     queue has been calm for a few samples. It never goes below a pool's declared minimum.</li>
 * </ul>
 *
 * <p>{@link #summary()} is the one printable line for an operator: it names the numbers and the pools, so
 * "we are degraded" never appears without "by how much and from whom". The record is immutable and safe
 * to hand to JFR or a command.
 *
 * @param criticalReserveCores cores the non critical pools may not take, at least zero
 * @param enabled              whether the load shedding policy is configured at all
 * @param shedCores            cores currently withheld from non critical pools, at least zero
 * @param pools                pools whose capacity is reduced right now, in id order
 * @param summary              one line with the numbers, never blank
 */
public record SheddingReport(
    int criticalReserveCores,
    boolean enabled,
    int shedCores,
    List<PoolId> pools,
    String summary
) {

    /**
     * Validates a shedding report.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a count is negative, a pool repeats, or the summary is blank
     */
    public SheddingReport {
        Objects.requireNonNull(pools, "pools");
        Objects.requireNonNull(summary, "summary");
        if (criticalReserveCores < 0) {
            throw new IllegalArgumentException(
                "criticalReserveCores must not be negative, got " + criticalReserveCores
            );
        }
        if (shedCores < 0) {
            throw new IllegalArgumentException("shedCores must not be negative, got " + shedCores);
        }
        if (summary.isBlank()) {
            throw new IllegalArgumentException("summary must not be blank");
        }
        pools = List.copyOf(pools);
    }

    /**
     * Creates the report of a controller that reserves nothing and sheds nothing.
     *
     * <p>Used by hand-built snapshots and by implementations that do not do capacity sharing. It is a
     * real answer, not a placeholder: the numbers are zero because nothing is held.
     *
     * @return the inactive report
     */
    public static SheddingReport none() {
        return new SheddingReport(0, false, 0, List.of(), "no critical reserve, load shedding off");
    }

    /**
     * Checks whether some capacity is withheld right now.
     *
     * @return {@code true} when {@link #shedCores()} is positive
     */
    public boolean active() {
        return this.shedCores > 0;
    }
}
