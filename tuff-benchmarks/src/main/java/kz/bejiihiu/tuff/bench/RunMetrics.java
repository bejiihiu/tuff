package kz.bejiihiu.tuff.bench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reduces the samples of one run to the summary the analyzer compares.
 *
 * <p>Only the measurement window is used. Warm-up samples stay in {@code ticks.csv} for diagnosis but
 * never enter a statistic, exactly like the methodology requires.
 *
 * <p>Every aggregate is per run, and a value is {@code NaN} when the corresponding field was not
 * observable in that window: the analyzer drops those values instead of treating them as zero.
 */
final class RunMetrics {

    /**
     * Source tag of the true primary metric: the scheduler's own per-tick events.
     */
    static final String JFR_SOURCE = "jfr_per_tick";

    /**
     * Source tag of the fallback: the sampled region aggregate.
     */
    static final String PROXY_SOURCE = "sampled_region_aggregate_proxy";

    /**
     * The rule behind the true {@code hard_miss_share}; written into every summary and report.
     */
    static final String JFR_DEFINITION =
        "a tick is a hard miss when the handle was more than one period behind (missedPeriods > 1), or when its "
            + "reconstructible lag reached one tick period (50 ms); skipped counts the periods the handle wrote off";

    /**
     * The rule behind the fallback metric; written into every summary and report.
     */
    static final String PROXY_DEFINITION =
        "a 5 s sample counts as a hard miss when min region TPS < 19.5 or the worst tick of the window >= 50 ms";

    /** TPS below which a region lost at least 2.5% of its periods inside the five second window. */
    private static final double HARD_MISS_TPS = 19.5d;

    /** A tick that took a full period is a missed deadline. */
    private static final double HARD_MISS_TICK_MILLIS = 50.0d;

    private RunMetrics() {
    }

    /**
     * Computes the summary of one run.
     *
     * @param options      run options, for the window and identity fields
     * @param arm          arm the run belongs to
     * @param runId        run identifier
     * @param benchId      block identifier
     * @param gitHead      git head of the checkout
     * @param jarSha       digest of the server jar
     * @param jdk          short JDK description
     * @param hostTag      host tag of the block
     * @param samples      samples of the run
     * @param jfr          facts of the measurement window
     * @param guardrails   guardrail findings
     * @param exitCode     server exit code
     * @param stopSeconds  graceful stop duration
     * @param measuredSeconds how much of the requested measurement window the samples cover
     * @param perTick      scheduler per-tick events of the window, or an absent result
     * @param perTickRequested whether the arm was supposed to produce per-tick events
     * @param extraReasons reasons found outside the metrics, such as a harness failure
     * @return the summary
     */
    static RunSummary compute(
        final BenchOptions options,
        final Arm arm,
        final String runId,
        final String benchId,
        final String gitHead,
        final String jarSha,
        final String jdk,
        final String hostTag,
        final List<Sample> samples,
        final JfrFacts.Facts jfr,
        final Guardrails.Result guardrails,
        final int exitCode,
        final double stopSeconds,
        final double measuredSeconds,
        final JfrTicks.Result perTick,
        final boolean perTickRequested,
        final List<String> extraReasons
    ) {
        final List<Sample> measure = samples.stream()
            .filter(sample -> sample.phase().equals("measure"))
            .filter(sample -> sample.region().present())
            .toList();
        final double[] minTps = values(measure, sample -> sample.region().minimumTps());
        final double[] averageMspt = values(measure, sample -> sample.region().averageMspt5s());
        final double[] peakMspt = values(measure, sample -> sample.region().maximumMspt5s());
        final double[] lagPercent = values(
            measure,
            sample -> sample.region().lagPercent() < 0 ? Double.NaN : sample.region().lagPercent()
        );
        final double[] regions = values(
            measure,
            sample -> sample.region().regions() < 0 ? Double.NaN : sample.region().regions()
        );
        final double[] entities = values(
            measure,
            sample -> sample.region().entities() < 0 ? Double.NaN : sample.region().entities()
        );
        final double[] players = values(
            measure,
            sample -> sample.region().players() < 0 ? Double.NaN : sample.region().players()
        );
        final double[] chunks = values(
            measure,
            sample -> sample.region().chunks() < 0 ? Double.NaN : sample.region().chunks()
        );

        int hardMisses = 0;
        int late = 0;
        for (final Sample sample : measure) {
            final Sample.RegionSample region = sample.region();
            final boolean slowTps = Double.isFinite(region.minimumTps()) && region.minimumTps() < HARD_MISS_TPS;
            final boolean slowTick = Double.isFinite(region.maximumMspt5s())
                && region.maximumMspt5s() >= HARD_MISS_TICK_MILLIS;
            if (slowTps || slowTick) {
                ++hardMisses;
            }
            if (region.lagPercent() > 0) {
                ++late;
            }
        }

        final List<Sample> funded = measure.stream().filter(sample -> sample.fund().present()).toList();
        final double[] live = funded.stream().mapToDouble(sample -> sample.fund().live()).toArray();
        final double[] queued = funded.stream().mapToDouble(sample -> sample.fund().queued()).toArray();
        final double[] overdue = funded.stream().mapToDouble(sample -> sample.fund().overdue()).toArray();
        final double spawnDelta = funded.isEmpty()
            ? Double.NaN
            : funded.getLast().fund().spawned() - funded.getFirst().fund().spawned();
        final double retireDelta = funded.isEmpty()
            ? Double.NaN
            : funded.getLast().fund().retired() - funded.getFirst().fund().retired();
        double threadSeconds = Double.NaN;
        if (!funded.isEmpty()) {
            threadSeconds = 0.0d;
            for (final double value : live) {
                threadSeconds += value * (options.sampleMillis / 1_000.0d);
            }
        }

        final long periodNanos = JfrTicks.TICK_PERIOD_NANOS;
        final int tickCount = perTick.ticks().size();
        int hardMissTicks = 0;
        long skippedPeriods = 0L;
        long catchUpTicks = 0L;
        int exactLagTicks = 0;
        final long[] lagNanos = new long[tickCount];
        final long[] tickMsptNanos = new long[tickCount];
        for (int index = 0; index < tickCount; ++index) {
            final JfrTicks.Tick tick = perTick.ticks().get(index);
            if (tick.lagExact()) {
                lagNanos[exactLagTicks++] = tick.lagNanos(periodNanos);
            }
            tickMsptNanos[index] = Math.max(0L, tick.executedNanos());
            if (tick.hardMiss(periodNanos)) {
                ++hardMissTicks;
            }
            if (tick.skipped() > 0L) {
                skippedPeriods += tick.skipped();
            }
            catchUpTicks += tick.catchUpTicks();
        }
        java.util.Arrays.sort(tickMsptNanos);
        final long[] exactLag = java.util.Arrays.copyOf(lagNanos, exactLagTicks);
        java.util.Arrays.sort(exactLag);
        final int lagInexactTicks = tickCount - exactLagTicks;
        final boolean trueMetric = tickCount > 0;
        final double proxyShare = measure.isEmpty() ? Double.NaN : hardMisses / (double) measure.size();
        final double hardMissShare = trueMetric ? hardMissTicks / (double) tickCount : proxyShare;
        final String source = trueMetric ? JFR_SOURCE : PROXY_SOURCE;
        final String definition = trueMetric ? JFR_DEFINITION : PROXY_DEFINITION;

        final List<String> reasons = new ArrayList<>(guardrails.reasons());
        reasons.addAll(extraReasons);
        if (measure.size() < 2) {
            reasons.add("insufficient_measurement_samples");
        }
        if (measuredSeconds + 1.0d < options.measureSeconds * 0.95d) {
            reasons.add("measurement_window_truncated");
        }
        if (measure.isEmpty() || Stats.median(regions) <= 0.0d) {
            reasons.add("no_ticking_regions");
        }
        if (!jfr.present()) {
            reasons.add("jfr_unavailable");
        }
        if (perTickRequested && !trueMetric) {
            reasons.add("no_per_tick_events");
        }
        final boolean valid = reasons.isEmpty();

        return new RunSummary(
            runId,
            arm.label(),
            options.profile.cli(),
            arm.scheduler(),
            arm.elastic(),
            options.threads,
            arm.elastic() ? options.maxThreads : options.threads,
            benchId,
            gitHead,
            jarSha,
            jdk,
            hostTag,
            options.warmupSeconds,
            options.measureSeconds,
            options.sampleMillis,
            source,
            definition,
            measure.size(),
            Stats.median(regions),
            Stats.median(entities),
            Stats.median(players),
            Stats.median(chunks),
            hardMisses,
            hardMissShare,
            measure.isEmpty() ? Double.NaN : late / (double) measure.size(),
            min(minTps),
            Stats.median(minTps),
            Stats.percentile(averageMspt, 0.50d),
            Stats.percentile(averageMspt, 0.95d),
            Stats.percentile(averageMspt, 0.99d),
            Stats.percentile(peakMspt, 0.99d),
            max(peakMspt),
            Stats.percentile(lagPercent, 0.50d),
            Stats.percentile(lagPercent, 0.95d),
            Stats.median(live),
            max(live),
            max(queued),
            max(overdue),
            spawnDelta,
            retireDelta,
            threadSeconds,
            jfr.present(),
            jfr.cpuUser(),
            jfr.cpuSystem(),
            jfr.gcPauseMillis(),
            jfr.gcPauseP99Millis(),
            jfr.heapUsedMaxMb(),
            jfr.present() ? jfr.threadStarts() : Double.NaN,
            jfr.present() ? jfr.threadEnds() : Double.NaN,
            exitCode,
            stopSeconds,
            guardrails.clean(),
            reasons,
            valid,
            perTickRequested,
            tickCount,
            hardMissTicks,
            proxyShare,
            skippedPeriods,
            catchUpTicks,
            periodNanos,
            Stats.percentile(exactLag, 0.50d),
            Stats.percentile(exactLag, 0.95d),
            Stats.percentile(exactLag, 0.99d),
            Stats.percentile(exactLag, 0.999d),
            exactLag.length == 0 ? Double.NaN : exactLag[exactLag.length - 1],
            Stats.percentile(tickMsptNanos, 0.99d),
            perTick.fund().size(),
            lagInexactTicks
        );
    }

    private interface Extractor {

        double value(Sample sample);
    }

    private static double[] values(final List<Sample> samples, final Extractor extractor) {
        final double[] values = new double[samples.size()];
        for (int index = 0; index < samples.size(); ++index) {
            values[index] = extractor.value(samples.get(index));
        }
        return Arrays.stream(values).filter(Double::isFinite).toArray();
    }

    private static double min(final double[] values) {
        return values.length == 0 ? Double.NaN : Arrays.stream(values).min().orElse(Double.NaN);
    }

    private static double max(final double[] values) {
        return values.length == 0 ? Double.NaN : Arrays.stream(values).max().orElse(Double.NaN);
    }
}
