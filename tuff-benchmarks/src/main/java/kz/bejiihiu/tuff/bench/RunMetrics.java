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
     * Explains where the primary metric comes from; written into every summary and report.
     */
    static final String HARD_MISS_SOURCE = "sampled_region_aggregate_proxy";

    /**
     * The rule behind {@code hard_miss_share}; written into every summary and report.
     */
    static final String HARD_MISS_DEFINITION =
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
            HARD_MISS_SOURCE,
            HARD_MISS_DEFINITION,
            measure.size(),
            Stats.median(regions),
            Stats.median(entities),
            Stats.median(players),
            Stats.median(chunks),
            hardMisses,
            measure.isEmpty() ? Double.NaN : hardMisses / (double) measure.size(),
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
            valid
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
