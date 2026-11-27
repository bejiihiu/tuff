package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

/**
 * Aggregates a block of runs into the comparison the methodology asks for.
 *
 * <p>The unit of analysis is a run. Each arm contributes one summary per run; the analyzer boots the
 * difference of means 10 000 times and reports the 95% percentile interval, never a bare "it got
 * better". Non-inferiority needs the upper bound of {@code arm - baseline} below a margin built from
 * the measured A/A noise and the signed policy delta, and the absolute stop (hard miss doubled) is
 * checked separately.
 *
 * <p>The primary metric has two possible sources. When both arms of a pair produced the scheduler's
 * per-tick events, the true hard miss is used; otherwise the pair is compared on the sampled fallback
 * that both arms can produce, and the row says so. A pair is never mixed: one arm measured per tick and
 * the other from a five second average would not be a comparison.
 *
 * <p>Invalid runs are excluded from the statistics and named in the report with their reasons. If the
 * block has no A/A pair the noise band is unknown and the report says every verdict is provisional.
 */
final class Analysis {

    private static final int POLICY_RUN_FLOOR = 10;

    private static final String TRUE_SOURCE = RunMetrics.JFR_SOURCE;
    private static final String PROXY_SOURCE = RunMetrics.PROXY_SOURCE;

    private static final List<Metric> METRICS = List.of(
        new Metric(
            "hard_miss_share", "hard miss share", false,
            RunSummary::hardMissShare, RunSummary::proxyHardMissShare
        ),
        new Metric(
            "skipped_periods", "skipped periods", false,
            summary -> summary.skippedPeriods(), null
        ),
        new Metric(
            "lag_p99_ns", "lag p99", false,
            summary -> summary.lagP99Nanos(), null
        ),
        new Metric(
            "tick_mspt_p99_ns", "tick body p99", false,
            summary -> summary.tickMsptP99Nanos(), null
        ),
        new Metric("late_share", "late sample share", false, RunSummary::lateShare, RunSummary::lateShare),
        new Metric("tps_min", "lowest region TPS", true, RunSummary::tpsMin, RunSummary::tpsMin),
        new Metric("mspt_avg_p99", "avg MSPT p99", false, RunSummary::msptAverageP99, RunSummary::msptAverageP99),
        new Metric("mspt_peak_p99", "worst tick p99", false, RunSummary::msptPeakP99, RunSummary::msptPeakP99),
        new Metric("thread_seconds", "thread seconds", false, RunSummary::threadSeconds, RunSummary::threadSeconds),
        new Metric("cpu_user", "JVM user CPU", false, RunSummary::cpuUser, RunSummary::cpuUser)
    );

    private Analysis() {
    }

    /**
     * Analyzes a block directory and writes its artifacts.
     *
     * @param benchDir block directory containing {@code runs/}
     * @param options  options that supply the baseline, margins and bootstrap size
     * @return paths and gate results
     * @throws IOException if the runs cannot be read or the artifacts cannot be written
     */
    static Result run(final Path benchDir, final BenchOptions options) throws IOException {
        final List<RunSummary> summaries = load(benchDir);
        if (summaries.isEmpty()) {
            throw new IOException("no run summary under " + benchDir.resolve("runs"));
        }
        final List<RunSummary> valid = summaries.stream().filter(RunSummary::valid).toList();
        final List<RunSummary> invalid = summaries.stream().filter(summary -> !summary.valid()).toList();
        final Map<String, List<RunSummary>> byArm = new LinkedHashMap<>();
        for (final Arm arm : Arm.values()) {
            byArm.put(arm.label(), valid.stream().filter(summary -> summary.arm().equals(arm.label())).toList());
        }

        final Map<String, Object> noise = noise(byArm, options);
        final double proxySigma = proxySigma(byArm);
        final List<Map<String, Object>> rows = new ArrayList<>();
        final List<RunSummary> baselineRuns = byArm.getOrDefault(options.baseline.label(), List.of());
        for (final Metric metric : METRICS) {
            for (final Arm arm : Arm.values()) {
                if (arm == options.baseline) {
                    continue;
                }
                if (arm == Arm.A2 && options.baseline == Arm.A) {
                    continue;
                }
                addPair(
                    rows, metric, arm.label(), byArm.getOrDefault(arm.label(), List.of()),
                    baselineRuns, options, proxySigma
                );
            }
            // the A/A row is calibration, not a treatment
            if (!byArm.getOrDefault(Arm.A2.label(), List.of()).isEmpty() && options.baseline == Arm.A) {
                final Map<String, Object> row = compare(
                    metric, "A2", byArm.get(Arm.A2.label()), baselineRuns, options, false, proxySigma
                );
                if (row != null) {
                    row.put("verdict", "noise_band");
                    rows.add(row);
                }
            }
        }
        // the hypothesis under test is C against B, and both arms can produce per-tick events
        for (final Metric metric : METRICS) {
            if (metric.proxyExtractor() != null && !metric.key().equals("hard_miss_share")) {
                continue;
            }
            addPair(
                rows,
                metric,
                Arm.C.label(),
                byArm.getOrDefault(Arm.C.label(), List.of()),
                byArm.getOrDefault(Arm.B.label(), List.of()),
                options,
                proxySigma
            );
        }

        final Map<String, String> gates = new LinkedHashMap<>();
        gates.put("G1_port_non_inferior", gateStatus(rows, "hard_miss_share", "B", "A", "non_inferior"));
        gates.put("G2_elastic_better_than_port", superiority(rows, "hard_miss_share", "C", "B"));
        gates.put("F1_port_worse_than_baseline", f1(rows));

        final Map<String, Object> json = new LinkedHashMap<>();
        final RunSummary first = summaries.getFirst();
        json.put("bench_id", first.benchId());
        json.put("generated_utc", Instant.now().toString());
        json.put("baseline", options.baseline.label());
        json.put("bootstrap", options.bootstrapSamples);
        json.put("seed", options.seed);
        json.put("margin_ratio", options.marginRatio);
        json.put("margin_hard_miss", options.marginHardMiss);
        json.put("environment", Map.of(
            "git_head", first.gitHead(),
            "jar_sha256", first.jarSha256(),
            "jdk", first.jdk(),
            "host_tag", first.hostTag(),
            "profile", first.profile(),
            "warmup_s", first.warmupSeconds(),
            "measure_s", first.measureSeconds(),
            "sample_ms", first.sampleMillis()
        ));
        json.put("sources", sources(summaries));
        json.put("definitions", Map.of(
            "true", RunMetrics.JFR_DEFINITION,
            "fallback", RunMetrics.PROXY_DEFINITION
        ));
        json.put("runs", summaries.stream().map(RunSummary::toMap).toList());
        json.put("invalid_runs", invalid.stream().map(summary -> Map.of(
            "run_id", summary.runId(),
            "arm", summary.arm(),
            "reasons", summary.invalidReasons()
        )).toList());
        json.put("noise", noise);
        json.put("metrics", rows);
        json.put("gates", gates);

        final Path csv = benchDir.resolve("summary.csv");
        writeCsv(csv, rows);
        Files.writeString(benchDir.resolve("compare.csv"), readString(csv), StandardCharsets.UTF_8);
        Json.write(benchDir.resolve("summary.json"), json);
        final Path report = benchDir.resolve("analysis.md");
        writeReport(report, rows, noise, gates, summaries, invalid, options);
        return new Result(report, csv, benchDir.resolve("summary.json"), valid.size(), invalid.size(), gates);
    }

    private static void addPair(
        final List<Map<String, Object>> rows,
        final Metric metric,
        final String arm,
        final List<RunSummary> armRuns,
        final List<RunSummary> baselineRuns,
        final BenchOptions options,
        final double proxySigma
    ) {
        if (armRuns.isEmpty() || baselineRuns.isEmpty()) {
            return;
        }
        final boolean truePair = hasPerTick(armRuns) && hasPerTick(baselineRuns);
        if (!truePair && metric.proxyExtractor() == null) {
            return;
        }
        final Map<String, Object> row = compare(metric, arm, armRuns, baselineRuns, options, truePair, proxySigma);
        if (row != null) {
            rows.add(row);
        }
    }

    private static Map<String, Object> compare(
        final Metric metric,
        final String arm,
        final List<RunSummary> armRuns,
        final List<RunSummary> baselineRuns,
        final BenchOptions options,
        final boolean truePair,
        final double proxySigma
    ) {
        final ToDoubleFunction<RunSummary> extractor = truePair ? metric.trueExtractor() : metric.proxyExtractor();
        if (extractor == null) {
            return null;
        }
        final double[] armValues = values(armRuns, extractor);
        final double[] baselineValues = values(baselineRuns, extractor);
        if (armValues.length == 0 || baselineValues.length == 0) {
            return null;
        }
        final double armMean = Stats.mean(armValues);
        final double baselineMean = Stats.mean(baselineValues);
        // the only honest noise band is the A/A spread on the fallback: the baseline arm cannot emit
        // per-tick events, so the true metric has no calibration pair in this block
        final double sigma = truePair ? 0.0d : proxySigma;
        final double policyDelta = policyDelta(metric, baselineMean, options);
        final double margin = Math.max(2.0d * sigma, policyDelta);
        final long seed = options.seed * 31L + metric.key().hashCode() * 1_000_003L + arm.hashCode();
        final Stats.Interval interval = Stats.bootstrapDifference(
            armValues, baselineValues, options.bootstrapSamples, seed
        );
        final String verdict = verdict(metric.higherIsBetter(), interval, margin);
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("metric", metric.key());
        row.put("arm", arm);
        row.put("baseline", baselineRuns.getFirst().arm());
        row.put("source", source(metric, truePair));
        row.put("n_arm", armValues.length);
        row.put("n_baseline", baselineValues.length);
        row.put("arm_mean", armMean);
        row.put("baseline_mean", baselineMean);
        row.put("delta", interval.mean());
        row.put("delta_rel", baselineMean == 0.0d ? Double.NaN : interval.mean() / Math.abs(baselineMean));
        row.put("ci_low", interval.low());
        row.put("ci_high", interval.high());
        row.put("margin", margin);
        row.put("sigma", sigma);
        row.put("verdict", verdict);
        return row;
    }

    private static double policyDelta(
        final Metric metric,
        final double baselineMean,
        final BenchOptions options
    ) {
        return switch (metric.key()) {
            case "hard_miss_share" -> options.marginHardMiss >= 0.0d
                ? options.marginHardMiss
                : Math.abs(baselineMean) * 0.01d;
            case "lag_p99_ns" -> 500_000.0d;
            default -> Math.abs(baselineMean) * options.marginRatio;
        };
    }

    private static String source(final Metric metric, final boolean truePair) {        if (metric.key().equals("hard_miss_share")) {
            return truePair ? TRUE_SOURCE : PROXY_SOURCE;
        }
        return metric.proxyExtractor() == null ? TRUE_SOURCE : "sampled_region_aggregate";
    }

    private static int intValue(final Object value) {
        return value instanceof final Number number ? number.intValue() : 0;
    }

    private static String verdict(final boolean higherIsBetter, final Stats.Interval interval, final double margin) {
        if (!interval.finite()) {
            return "insufficient_data";
        }
        if (higherIsBetter) {
            if (interval.low() > 0.0d) {
                return "better";
            }
            if (interval.low() >= -margin) {
                return "non_inferior";
            }
            if (interval.high() < -margin) {
                return "worse";
            }
            return "inconclusive";
        }
        if (interval.high() < 0.0d) {
            return "better";
        }
        if (interval.high() <= margin) {
            return "non_inferior";
        }
        if (interval.low() > margin) {
            return "worse";
        }
        return "inconclusive";
    }

    private static double[] values(final List<RunSummary> runs, final ToDoubleFunction<RunSummary> extractor) {
        return runs.stream()
            .mapToDouble(extractor)
            .filter(Double::isFinite)
            .toArray();
    }

    private static double proxySigma(final Map<String, List<RunSummary>> byArm) {
        final double[] baseline = values(
            byArm.getOrDefault(Arm.A.label(), List.of()), RunSummary::proxyHardMissShare
        );
        final double[] repeat = values(
            byArm.getOrDefault(Arm.A2.label(), List.of()), RunSummary::proxyHardMissShare
        );
        if (baseline.length == 0 || repeat.length == 0) {
            return 0.0d;
        }
        return sigma(baseline, repeat);
    }

    private static boolean hasPerTick(final List<RunSummary> runs) {
        return !runs.isEmpty() && runs.stream().allMatch(summary -> summary.ticks() > 0);
    }

    private static double sigma(final double[] left, final double[] right) {
        final double[] pooled = new double[left.length + right.length];
        System.arraycopy(left, 0, pooled, 0, left.length);
        System.arraycopy(right, 0, pooled, left.length, right.length);
        return Stats.standardDeviation(pooled);
    }

    private static Map<String, Object> noise(
        final Map<String, List<RunSummary>> byArm,
        final BenchOptions options
    ) {
        final Map<String, Object> noise = new LinkedHashMap<>();
        final List<RunSummary> baseline = byArm.getOrDefault(Arm.A.label(), List.of());
        final List<RunSummary> repeat = byArm.getOrDefault(Arm.A2.label(), List.of());
        noise.put("aa_present", !baseline.isEmpty() && !repeat.isEmpty());
        noise.put("n_baseline", baseline.size());
        noise.put("n_repeat", repeat.size());
        if (!baseline.isEmpty() && !repeat.isEmpty()) {
            final double[] baselineValues = values(baseline, RunSummary::proxyHardMissShare);
            final double[] repeatValues = values(repeat, RunSummary::proxyHardMissShare);
            final Stats.Interval interval = Stats.bootstrapDifference(
                repeatValues,
                baselineValues,
                options.bootstrapSamples,
                options.seed * 17L
            );
            final double sigma = sigma(baselineValues, repeatValues);
            final double policy = options.marginHardMiss >= 0.0d
                ? options.marginHardMiss
                : Math.abs(Stats.mean(baselineValues)) * 0.01d;
            final double margin = Math.max(2.0d * sigma, policy);
            noise.put("hard_miss_delta_low", interval.low());
            noise.put("hard_miss_delta_mean", interval.mean());
            noise.put("hard_miss_delta_high", interval.high());
            noise.put("sigma_hard_miss", sigma);
            noise.put("margin_hard_miss", margin);
            final boolean within = interval.finite()
                && Math.abs(interval.low()) <= margin
                && Math.abs(interval.high()) <= margin;
            noise.put("calibration", within ? "ok" : "noisy");
        } else {
            noise.put("calibration", "missing");
        }
        return noise;
    }

    /**
     * Loads every run summary of a block.
     *
     * @param benchDir block directory
     * @return summaries, sorted by run id
     * @throws IOException if a summary cannot be read
     */
    static List<RunSummary> load(final Path benchDir) throws IOException {
        final Path runs = benchDir.resolve("runs");
        if (!Files.isDirectory(runs)) {
            return List.of();
        }
        final List<RunSummary> summaries = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(runs)) {
            for (final Path summary : walk.filter(path -> path.getFileName().toString().equals("summary.json")).toList()) {
                final Object parsed = Json.read(summary);
                if (parsed instanceof final Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> typed = (Map<String, Object>) map;
                    summaries.add(RunSummary.fromMap(typed));
                }
            }
        }
        summaries.sort((left, right) -> left.runId().compareTo(right.runId()));
        return List.copyOf(summaries);
    }

    /**
     * Returns the measurement source of every arm that has valid runs.
     *
     * @param summaries valid runs
     * @return arm label to source tag
     */
    private static Map<String, String> sources(final List<RunSummary> summaries) {
        final Map<String, java.util.Set<String>> byArm = new LinkedHashMap<>();
        for (final RunSummary summary : summaries) {
            byArm.computeIfAbsent(summary.arm(), _ -> new java.util.LinkedHashSet<>())
                .add(summary.hardMissSource());
        }
        final Map<String, String> sources = new LinkedHashMap<>();
        byArm.forEach((arm, tags) -> sources.put(arm, String.join("+", tags)));
        return sources;
    }

    private static String gateStatus(
        final List<Map<String, Object>> rows,
        final String metric,
        final String arm,
        final String baseline,
        final String accepted
    ) {
        for (final Map<String, Object> row : rows) {
            if (metric.equals(row.get("metric")) && arm.equals(row.get("arm"))
                && baseline.equals(row.get("baseline"))) {
                if (intValue(row.get("n_arm")) < 2 || intValue(row.get("n_baseline")) < 2) {
                    return "INSUFFICIENT_DATA";
                }
                final String verdict = String.valueOf(row.get("verdict"));
                return accepted.equals(verdict) || "better".equals(verdict) ? "PASS" : "FAIL(" + verdict + ')';
            }
        }
        return "INSUFFICIENT_DATA";
    }

    private static String superiority(
        final List<Map<String, Object>> rows,
        final String metric,
        final String arm,
        final String reference
    ) {
        for (final Map<String, Object> row : rows) {
            if (metric.equals(row.get("metric")) && arm.equals(row.get("arm"))
                && reference.equals(row.get("baseline"))) {
                if (intValue(row.get("n_arm")) < 2 || intValue(row.get("n_baseline")) < 2) {
                    return "INSUFFICIENT_DATA";
                }
                final String verdict = String.valueOf(row.get("verdict"));
                return "better".equals(verdict) ? "PASS" : "NOT_DEMONSTRATED(" + verdict + ')';
            }
        }
        return "INSUFFICIENT_DATA";
    }

    private static String f1(final List<Map<String, Object>> rows) {
        for (final Map<String, Object> row : rows) {
            if ("hard_miss_share".equals(row.get("metric")) && "B".equals(row.get("arm"))
                && "A".equals(row.get("baseline"))) {
                return "worse".equals(row.get("verdict")) ? "TRIGGERED" : "not_triggered";
            }
        }
        return "INSUFFICIENT_DATA";
    }

    private static void writeCsv(final Path file, final List<Map<String, Object>> rows) throws IOException {
        final StringBuilder out = new StringBuilder(2_048);
        out.append("metric,arm,baseline,source,n_arm,n_baseline,arm_mean,baseline_mean,delta,delta_rel,"
            + "ci_low,ci_high,margin,sigma,verdict\n");
        for (final Map<String, Object> row : rows) {
            out.append(row.get("metric")).append(',')
                .append(row.get("arm")).append(',')
                .append(row.get("baseline")).append(',')
                .append(row.get("source")).append(',')
                .append(row.get("n_arm")).append(',')
                .append(row.get("n_baseline")).append(',')
                .append(csvNumber(row.get("arm_mean"))).append(',')
                .append(csvNumber(row.get("baseline_mean"))).append(',')
                .append(csvNumber(row.get("delta"))).append(',')
                .append(csvNumber(row.get("delta_rel"))).append(',')
                .append(csvNumber(row.get("ci_low"))).append(',')
                .append(csvNumber(row.get("ci_high"))).append(',')
                .append(csvNumber(row.get("margin"))).append(',')
                .append(csvNumber(row.get("sigma"))).append(',')
                .append(row.get("verdict"))
                .append('\n');
        }
        final Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(file, out, StandardCharsets.UTF_8);
    }

    private static String csvNumber(final Object value) {
        if (value instanceof final Number number) {
            return Double.isFinite(number.doubleValue())
                ? String.format(Locale.ROOT, "%.6f", number.doubleValue())
                : "";
        }
        return "";
    }

    private static String readString(final Path file) throws IOException {
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    private static void writeReport(
        final Path file,
        final List<Map<String, Object>> rows,
        final Map<String, Object> noise,
        final Map<String, String> gates,
        final List<RunSummary> summaries,
        final List<RunSummary> invalid,
        final BenchOptions options
    ) throws IOException {
        final StringBuilder out = new StringBuilder(16_384);
        final RunSummary first = summaries.getFirst();
        out.append("# Scheduler A/B block ").append(first.benchId()).append("\n\n");
        out.append("Generated by the bench harness; do not edit by hand. Machine readable detail: ")
            .append("`summary.json`, per-metric table: `compare.csv`.\n\n");

        out.append("## Environment\n\n");
        out.append("| | |\n|---|---|\n");
        Artifacts.row(out, "git head", first.gitHead());
        Artifacts.row(out, "jar sha256", first.jarSha256());
        Artifacts.row(out, "jdk", first.jdk());
        Artifacts.row(out, "host", first.hostTag());
        Artifacts.row(out, "profile", first.profile());
        Artifacts.row(out, "warm-up / measurement", first.warmupSeconds() + " s / " + first.measureSeconds() + " s");
        Artifacts.row(out, "sample interval", first.sampleMillis() + " ms");
        Artifacts.row(out, "baseline arm", options.baseline.label());
        Artifacts.row(out, "bootstrap resamples", Integer.toString(options.bootstrapSamples));
        out.append('\n');

        out.append("## Measurement sources\n\n");
        out.append("| arm | primary metric source | per-tick runs | ticks in this run |\n");
        out.append("|---|---|---|---|\n");
        for (final RunSummary summary : summaries) {
            out.append("| ").append(summary.arm())
                .append(" | `").append(summary.hardMissSource()).append('`')
                .append(" | ").append(summary.ticks() > 0 ? "1" : "0")
                .append(" | ").append(summary.ticks())
                .append(" |\n");
        }
        out.append('\n');
        out.append("True definition: ").append(RunMetrics.JFR_DEFINITION).append(".\n\n");
        out.append("Fallback definition: ").append(RunMetrics.PROXY_DEFINITION).append(".\n\n");
        out.append("A pair is compared on the true metric only when both arms produced per-tick events. ")
            .append("An EDF arm has no per-tick source, so comparisons against it use the fallback that both ")
            .append("arms can produce; the `source` column of every row says which one was used.\n\n");

        out.append("## Runs\n\n");
        out.append("| run | arm | threads | valid | hard miss | ticks | skipped | p99 avg MSPT | min TPS | exit |\n");
        out.append("|---|---|---|---|---|---|---|---|---|---|\n");
        for (final RunSummary summary : summaries) {
            out.append("| ").append(summary.runId())
                .append(" | ").append(summary.arm())
                .append(" | ").append(summary.threads()).append('/').append(summary.maxThreads())
                .append(" | ").append(summary.valid() ? "yes" : "no")
                .append(" | ").append(Artifacts.percent(summary.hardMissShare()))
                .append(" | ").append(summary.ticks())
                .append(" | ").append(summary.skippedPeriods())
                .append(" | ").append(Artifacts.millis(summary.msptAverageP99()))
                .append(" | ").append(Artifacts.decimal(summary.tpsMin()))
                .append(" | ").append(summary.exitCode())
                .append(" |\n");
        }
        out.append('\n');
        out.append("Valid runs: ").append(summaries.size() - invalid.size())
            .append(", invalid: ").append(invalid.size()).append(".\n\n");
        if (!invalid.isEmpty()) {
            out.append("Invalid runs were excluded from every statistic:\n\n");
            for (final RunSummary summary : invalid) {
                out.append("- `").append(summary.runId()).append("` (")
                    .append(summary.arm()).append("): ")
                    .append(String.join(", ", summary.invalidReasons()))
                    .append('\n');
            }
            out.append('\n');
        }

        out.append("## A/A calibration\n\n");
        if (Boolean.TRUE.equals(noise.get("aa_present"))) {
            out.append("The baseline arm was measured twice on the sampled fallback (the only source an EDF ")
                .append("arm can produce). The noise band on the primary metric is ");
            out.append('[').append(Artifacts.decimal(noise.get("hard_miss_delta_low") instanceof final Number low
                ? low.doubleValue() : Double.NaN)).append(", ")
                .append(Artifacts.decimal(noise.get("hard_miss_delta_high") instanceof final Number high
                    ? high.doubleValue() : Double.NaN)).append("] ");
            out.append("(A2 - A), sigma ").append(Artifacts.decimal(noise.get("sigma_hard_miss") instanceof final Number s
                ? s.doubleValue() : Double.NaN)).append(", margin ")
                .append(Artifacts.decimal(noise.get("margin_hard_miss") instanceof final Number m
                    ? m.doubleValue() : Double.NaN)).append(".\n\n");
            out.append("Calibration: `").append(noise.get("calibration")).append("`.")
                .append(" The band is a band of the stand on the fallback metric; no A/A pair exists on the ")
                .append("true metric because the baseline arm does not emit per-tick events.\n\n");
        } else {
            out.append("This block has no A/A pair, so the noise band is unknown and every verdict below is ")
                .append("provisional.\n\n");
        }

        out.append("## Primary metric: hard miss\n\n");
        writeMetricTable(out, rows, "hard_miss_share");
        out.append("The `source` column names the definition that produced the row. `C vs B` is the ")
            .append("elasticity test and is measured per tick on both arms; `vs A` is the port check and uses ")
            .append("the fallback because EDF has no per-tick event source.\n\n");

        out.append("## Secondary metrics\n\n");
        for (final Metric metric : METRICS) {
            if (metric.key().equals("hard_miss_share")) {
                continue;
            }
            out.append("### ").append(metric.label()).append("\n\n");
            writeMetricTable(out, rows, metric.key());
        }

        out.append("## Pre-registered gates\n\n");
        out.append("| gate | result |\n|---|---|\n");
        for (final Map.Entry<String, String> gate : gates.entrySet()) {
            Artifacts.row(out, gate.getKey(), gate.getValue());
        }
        out.append('\n');

        out.append("## Verdict\n\n");
        final boolean policyFloor = options.suiteArms.stream().allMatch(arm -> summaries.stream()
            .filter(RunSummary::valid)
            .filter(summary -> summary.arm().equals(arm.label()))
            .count() >= POLICY_RUN_FLOOR);
        if (!policyFloor) {
            out.append("This block is a **pilot**: at least one arm has fewer than ")
                .append(POLICY_RUN_FLOOR).append(" valid runs, so the confidence intervals are wide and no ")
                .append("default-on decision may be taken from it.\n\n");
        }
        out.append("G1 (the port is non-inferior to the baseline) is `").append(gates.get("G1_port_non_inferior"))
            .append("`; G2 (elasticity is a measurable improvement over the port, measured per tick) is `")
            .append(gates.get("G2_elastic_better_than_port")).append("`. F1 (the port is worse than the baseline ")
            .append("on the primary metric) is `").append(gates.get("F1_port_worse_than_baseline")).append("`.\n\n");
        out.append("A positive verdict requires the A/A calibration above, the policy floor of ")
            .append(POLICY_RUN_FLOOR).append(" runs per arm and a guardrail-clean block. Anything else is ")
            .append("evidence about the stand, not about the scheduler.\n\n");

        out.append("## What this block does not measure\n\n");
        out.append("- An EDF arm has no per-tick event source, so its hard miss is the sampled fallback; the ")
            .append("true per-tick metric exists on the Kairos arms only.\n");
        out.append("- No client bots: regions come from force-loaded areas, so packets, players and their ")
            .append("chunk load are absent.\n");
        out.append("- The compute lane cannot be loaded without a bench plugin, so the `idle-compute` profile ")
            .append("measures an idle server.\n");
        out.append("- Nothing here says anything about Folia upstream; the baseline is this build with ")
            .append("`scheduler: EDF`.\n");
        Files.writeString(file, out, StandardCharsets.UTF_8);
    }

    private static void writeMetricTable(
        final StringBuilder out,
        final List<Map<String, Object>> rows,
        final String metric
    ) {
        out.append("| arm | vs | source | n | mean | baseline | delta | 95% CI | margin | verdict |\n");
        out.append("|---|---|---|---|---|---|---|---|---|---|\n");
        for (final Map<String, Object> row : rows) {
            if (!metric.equals(row.get("metric"))) {
                continue;
            }
            out.append("| ").append(row.get("arm"))
                .append(" | ").append(row.get("baseline"))
                .append(" | ").append(row.get("source"))
                .append(" | ").append(row.get("n_arm"))
                .append(" | ").append(number(row.get("arm_mean")))
                .append(" | ").append(number(row.get("baseline_mean")))
                .append(" | ").append(number(row.get("delta")))
                .append(" | [").append(number(row.get("ci_low"))).append(", ").append(number(row.get("ci_high"))).append(']')
                .append(" | ").append(number(row.get("margin")))
                .append(" | ").append(row.get("verdict"))
                .append(" |\n");
        }
        out.append('\n');
    }

    private static String number(final Object value) {
        return value instanceof final Number number && Double.isFinite(number.doubleValue())
            ? String.format(Locale.ROOT, "%.6f", number.doubleValue())
            : "n/a";
    }

    /**
     * One extracted metric and the two possible sources.
     *
     * @param key            artifact column name
     * @param label          human readable name
     * @param higherIsBetter whether an increase is an improvement
     * @param trueExtractor  reads the metric from per-tick events
     * @param proxyExtractor reads the fallback value, or {@code null} when the metric only exists per tick
     */
    private record Metric(
        String key,
        String label,
        boolean higherIsBetter,
        ToDoubleFunction<RunSummary> trueExtractor,
        ToDoubleFunction<RunSummary> proxyExtractor
    ) {
    }

    /**
     * What the analysis produced.
     *
     * @param report      {@code analysis.md}
     * @param table       {@code summary.csv}
     * @param json        {@code summary.json}
     * @param validRuns   number of valid runs used
     * @param invalidRuns number of invalid runs excluded
     * @param gates       pre-registered gate results
     */
    record Result(
        Path report,
        Path table,
        Path json,
        int validRuns,
        int invalidRuns,
        Map<String, String> gates
    ) {
    }
}
