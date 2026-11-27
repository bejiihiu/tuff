package kz.bejiihiu.tuff.bench;

import java.util.List;

/**
 * One timestamped console observation of the running server.
 *
 * <p>The stand cannot see individual ticks today, so one sample is one read of the two real telemetry
 * surfaces that exist: the sampled per-region aggregate printed by {@code /tuff threads} and the fund
 * counters printed by {@code /tuff scheduler}. Fields the server did not answer are {@code NaN} or
 * {@code -1}, never zero, so an unavailable measurement cannot be mistaken for a good one.
 *
 * @param seq          marker sequence, unique inside a run
 * @param phase        {@code warmup}, {@code measure} or {@code last}
 * @param epochMillis  wall clock when the marker was sent
 * @param elapsedMillis time since the run's start marker
 * @param region       region aggregate, or {@link RegionSample#ABSENT}
 * @param fund         fund counters, or {@link FundSample#ABSENT}
 * @param scheduler     scheduler report fields
 * @param raw           console lines between this marker and the next one
 */
record Sample(
    long seq,
    String phase,
    long epochMillis,
    long elapsedMillis,
    RegionSample region,
    FundSample fund,
    SchedulerSample scheduler,
    List<String> raw
) {

    /**
     * Validates the sample and freezes the raw lines.
     */
    Sample {
        raw = List.copyOf(raw);
    }

    /**
     * Per-region aggregate printed by {@code /tuff threads} for the region pool.
     *
     * @param regions          number of ticking regions the pass saw
     * @param averageMspt5s    mean MSPT over the last five seconds, or {@code NaN}
     * @param maximumMspt5s    worst single tick over the last five seconds, or {@code NaN}
     * @param averageMspt15s   mean MSPT over the last fifteen seconds, or {@code NaN}
     * @param maximumMspt15s   worst single tick over the last fifteen seconds, or {@code NaN}
     * @param minimumTps       lowest one minute TPS across regions, or {@code NaN}
     * @param lagPercent       share of regions considered lagging, 0 to 100, or {@code -1}
     * @param laggingRegions   number of lagging regions, or {@code -1}
     * @param entities         entities seen by the pass, or {@code -1}
     * @param players          players seen by the pass, or {@code -1}
     * @param chunks           loaded chunks seen by the pass, or {@code -1}
     */
    record RegionSample(
        int regions,
        double averageMspt5s,
        double maximumMspt5s,
        double averageMspt15s,
        double maximumMspt15s,
        double minimumTps,
        int lagPercent,
        int laggingRegions,
        long entities,
        long players,
        long chunks
    ) {

        /** The value used when the pass printed no region line at all. */
        static final RegionSample ABSENT = new RegionSample(
            -1, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, -1, -1, -1L, -1L, -1L
        );

        /**
         * Returns whether the pass reported at least one region.
         *
         * @return {@code true} when the field set is real
         */
        boolean present() {
            return this.regions >= 0;
        }
    }

    /**
     * Fund counters printed by {@code /tuff scheduler} under Kairos.
     *
     * @param min              floor of the fund
     * @param live             live runners
     * @param target           size the last supervisor pass asked for
     * @param max              resolved ceiling
     * @param queued           ticks waiting for a permit
     * @param awaiting         runners holding a permit and waiting for the deadline
     * @param executing        runners inside a tick body
     * @param overdue          queued ticks past the wait threshold
     * @param oldestOverdueMs  age of the oldest overdue tick in milliseconds
     * @param waitAverageMs    mean positive wait in milliseconds
     * @param waitMaximumMs    longest positive wait in milliseconds
     * @param spawned          runners started since process start
     * @param retired          runners retired since process start
     */
    record FundSample(
        int min,
        int live,
        int target,
        int max,
        int queued,
        int awaiting,
        int executing,
        int overdue,
        long oldestOverdueMs,
        double waitAverageMs,
        double waitMaximumMs,
        long spawned,
        long retired
    ) {

        /** The value used on arms without fund telemetry. */
        static final FundSample ABSENT = new FundSample(
            -1, -1, -1, -1, -1, -1, -1, -1, -1L, Double.NaN, Double.NaN, -1L, -1L
        );

        /**
         * Returns whether the fund line was present.
         *
         * @return {@code true} for a Kairos arm
         */
        boolean present() {
            return this.min >= 0;
        }
    }

    /**
     * Scheduler identity and health as the reports describe it.
     *
     * @param name      scheduler name as reported, or the empty string
     * @param supervisor supervisor state word, or the empty string
     * @param status    fund status word, or the empty string
     * @param degraded  comma separated degraded reasons, or the empty string
     */
    record SchedulerSample(String name, String supervisor, String status, String degraded) {

        /** The value used when neither command answered. */
        static final SchedulerSample ABSENT = new SchedulerSample("", "", "", "");

        /**
         * Returns whether any scheduler information was captured.
         *
         * @return {@code true} when at least the name is known
         */
        boolean present() {
            return !this.name.isEmpty();
        }
    }
}
