package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Writes the per-run artifact set: the raw series, the console snapshots and the run report.
 *
 * <p>Columns are stable and snake case, one fact per column, no aggregation in the CSV. The report is
 * rendered from the same objects the CSVs were written from, so a quote in the report can always be
 * traced back to a row.
 *
 * <p>The report always states what could not be measured. A stand that quietly omits a metric is worse
 * than one that names the gap.
 */
final class Artifacts {

    private static final String TICKS_HEADER =
        "run_id,seq,phase,epoch_ms,elapsed_ms,regions,avg_mspt_5s,max_mspt_5s,avg_mspt_15s,"
            + "max_mspt_15s,min_tps,lag_pct,lagging_regions,entities,players,chunks";

    private static final String FUND_HEADER =
        "run_id,seq,phase,epoch_ms,min,live,target,max,queued,awaiting,executing,overdue,"
            + "oldest_overdue_ms,wait_avg_ms,wait_max_ms,spawned,retired";

    private Artifacts() {
    }

    /**
     * Writes the per-sample region series.
     *
     * @param file  destination
     * @param runId run identifier
     * @param samples samples of the run
     * @throws IOException if the file cannot be written
     */
    static void writeTicks(final Path file, final String runId, final List<Sample> samples) throws IOException {
        final StringBuilder out = new StringBuilder(4_096);
        out.append(TICKS_HEADER).append('\n');
        for (final Sample sample : samples) {
            final Sample.RegionSample region = sample.region();
            out.append(escape(runId)).append(',')
                .append(sample.seq()).append(',')
                .append(sample.phase()).append(',')
                .append(sample.epochMillis()).append(',')
                .append(sample.elapsedMillis()).append(',')
                .append(region.regions()).append(',')
                .append(number(region.averageMspt5s())).append(',')
                .append(number(region.maximumMspt5s())).append(',')
                .append(number(region.averageMspt15s())).append(',')
                .append(number(region.maximumMspt15s())).append(',')
                .append(number(region.minimumTps())).append(',')
                .append(region.lagPercent()).append(',')
                .append(region.laggingRegions()).append(',')
                .append(region.entities()).append(',')
                .append(region.players()).append(',')
                .append(region.chunks())
                .append('\n');
        }
        write(file, out);
    }

    /**
     * Writes the per-sample fund series.
     *
     * @param file  destination
     * @param runId run identifier
     * @param samples samples of the run
     * @throws IOException if the file cannot be written
     */
    static void writeFund(final Path file, final String runId, final List<Sample> samples) throws IOException {
        final StringBuilder out = new StringBuilder(4_096);
        out.append(FUND_HEADER).append('\n');
        for (final Sample sample : samples) {
            final Sample.FundSample fund = sample.fund();
            out.append(escape(runId)).append(',')
                .append(sample.seq()).append(',')
                .append(sample.phase()).append(',')
                .append(sample.epochMillis()).append(',')
                .append(fund.min()).append(',')
                .append(fund.live()).append(',')
                .append(fund.target()).append(',')
                .append(fund.max()).append(',')
                .append(fund.queued()).append(',')
                .append(fund.awaiting()).append(',')
                .append(fund.executing()).append(',')
                .append(fund.overdue()).append(',')
                .append(fund.oldestOverdueMs()).append(',')
                .append(number(fund.waitAverageMs())).append(',')
                .append(number(fund.waitMaximumMs())).append(',')
                .append(fund.spawned()).append(',')
                .append(fund.retired())
                .append('\n');
        }
        write(file, out);
    }

    /**
     * Writes the raw console block of selected samples.
     *
     * <p>Every measurement sample would be a lot of duplicated log; the first and last sample of each
     * phase plus every tenth sample are kept, which is enough to audit a report.
     *
     * @param directory snapshot directory
     * @param result    parsed samples with raw blocks
     * @param phase     phase to keep
     * @throws IOException if a file cannot be written
     */
    static void writeSnapshots(
        final Path directory,
        final SampleParser.Result result,
        final String phase
    ) throws IOException {
        Files.createDirectories(directory);
        final List<Sample> samples = result.samples().stream()
            .filter(sample -> sample.phase().equals(phase))
            .toList();
        for (int index = 0; index < samples.size(); ++index) {
            final boolean boundary = index == 0 || index == samples.size() - 1;
            if (!boundary && index % 10 != 0) {
                continue;
            }
            final Sample sample = samples.get(index);
            final String name = String.format(Locale.ROOT, "%05d-%s.txt", sample.seq(), sample.phase());
            write(directory.resolve(name), new StringBuilder().append(String.join("\n", sample.raw())).append('\n'));
        }
    }

    /**
     * Renders the report of one run.
     *
     * @param file    destination
     * @param summary computed summary
     * @param jfr     JFR facts
     * @param guardrails guardrail findings
     * @param flags   JVM flags the server ran with
     * @throws IOException if the file cannot be written
     */
    static void writeRunReport(
        final Path file,
        final RunSummary summary,
        final JfrFacts.Facts jfr,
        final Guardrails.Result guardrails,
        final List<String> flags
    ) throws IOException {
        final StringBuilder out = new StringBuilder(8_192);
        out.append("# Run ").append(summary.runId()).append("\n\n");
        out.append("Generated by the bench harness; do not edit by hand.\n\n");
        out.append("| | |\n|---|---|\n");
        row(out, "arm", summary.arm() + " (" + summary.scheduler() + ')');
        row(out, "profile", summary.profile());
        row(out, "bench id", summary.benchId());
        row(out, "git head", summary.gitHead());
        row(out, "jar sha256", summary.jarSha256());
        row(out, "jdk", summary.jdk());
        row(out, "host", summary.hostTag());
        row(out, "region threads", summary.threads() + ", ceiling " + summary.maxThreads());
        row(out, "warm-up / measurement", summary.warmupSeconds() + " s / " + summary.measureSeconds() + " s");
        row(out, "sample interval", summary.sampleMillis() + " ms");
        row(out, "exit code", Integer.toString(summary.exitCode()));
        row(out, "graceful stop", seconds(summary.stopSeconds()));
        out.append('\n');

        out.append("## Primary metric\n\n");
        out.append("`hard miss` source: `").append(summary.hardMissSource()).append("`.\n\n");
        out.append("A sample is a hard miss when ").append(summary.hardMissDefinition()).append(".\n\n");
        out.append("| metric | value |\n|---|---|\n");
        row(out, "samples in window", Integer.toString(summary.samples()));
        row(out, "hard miss samples", Integer.toString(summary.hardMissSamples()));
        row(out, "hard miss share", percent(summary.hardMissShare()));
        row(out, "late sample share", percent(summary.lateShare()));
        out.append('\n');

        out.append("## Secondary metrics\n\n");
        out.append("| metric | value |\n|---|---|\n");
        row(out, "lowest region TPS", decimal(summary.tpsMin()));
        row(out, "median region TPS", decimal(summary.tpsP50()));
        row(out, "avg MSPT p50", millis(summary.msptAverageP50()));
        row(out, "avg MSPT p95", millis(summary.msptAverageP95()));
        row(out, "avg MSPT p99", millis(summary.msptAverageP99()));
        row(out, "worst tick p99", millis(summary.msptPeakP99()));
        row(out, "worst tick max", millis(summary.msptPeakMax()));
        row(out, "lagging region share p95", percent(summary.lagPercentP95() / 100.0d));
        row(out, "regions (median)", decimal(summary.regionsMedian()));
        row(out, "entities (median)", decimal(summary.entitiesMedian()));
        row(out, "chunks (median)", decimal(summary.chunksMedian()));
        row(out, "fund live median / max", decimal(summary.fundLiveMedian()) + " / " + decimal(summary.fundLiveMax()));
        row(out, "fund queued max", decimal(summary.fundQueuedMax()));
        row(out, "fund overdue max", decimal(summary.fundOverdueMax()));
        row(out, "spawn / retire in window", decimal(summary.spawnDelta()) + " / " + decimal(summary.retireDelta()));
        row(out, "thread seconds", decimal(summary.threadSeconds()));
        out.append('\n');

        out.append("## JVM window facts\n\n");
        if (jfr.present()) {
            out.append("| metric | value |\n|---|---|\n");
            row(out, "JVM user CPU (fraction)", decimal(summary.cpuUser()));
            row(out, "JVM system CPU (fraction)", decimal(summary.cpuSystem()));
            row(out, "GC pauses", Long.toString(jfr.gcCount()));
            row(out, "GC pause total", millis(summary.gcPauseMillis()));
            row(out, "GC pause p99", millis(summary.gcPauseP99Millis()));
            row(out, "heap used max (at GC)", decimal(summary.heapUsedMaxMb()) + " MiB");
            row(out, "threads started / ended", decimal(summary.threadStarts()) + " / " + decimal(summary.threadEnds()));
            if (jfr.gcCount() == 0L) {
                out.append("\nNo GC paused in the measurement window, so no pause or heap-at-GC fact exists; ")
                    .append("the zeros above are real, the n/a entries are not zeros.\n");
            }
            out.append("\nTop execution-sample threads:\n\n");
            jfr.topThreads().forEach((name, count) -> out.append("- ").append(name).append(": ").append(count).append('\n'));
        } else {
            out.append("JFR recording unavailable: ").append(jfr.reason()).append('\n');
        }
        out.append('\n');

        out.append("## Guardrails and validity\n\n");
        out.append(summary.valid() ? "The run is valid.\n\n" : "The run is invalid.\n\n");
        if (!summary.invalidReasons().isEmpty()) {
            out.append("Reasons:\n\n");
            for (final String reason : summary.invalidReasons()) {
                out.append("- `").append(reason).append('`');
                final String evidence = guardrails.evidence().get(reason);
                if (evidence != null) {
                    out.append(": ").append(evidence);
                }
                out.append('\n');
            }
            out.append('\n');
        }

        out.append("## What is not observable in this setup\n\n");
        out.append("- Per-tick `scheduledStart`, `tickStart` and `periodsAhead` are not exposed by the running "
            + "server; the primary metric is a sample-level proxy over the real region aggregate.\n");
        out.append("- No client bots are used: regions come from force-loaded areas, so packet and player load "
            + "are absent.\n");
        out.append("- The `idle-compute` profile cannot load the compute lane without a bench plugin.\n\n");

        out.append("## Files\n\n");
        out.append("- `manifest.json` - frozen environment and window boundaries\n");
        out.append("- `server.log` - captured console output\n");
        out.append("- `gc.log` - JVM GC log\n");
        out.append("- `jfr/recording.jfr` - profiling recording\n");
        out.append("- `ticks.csv` - per-sample region aggregate\n");
        out.append("- `fund.csv` - per-sample fund counters\n");
        out.append("- `snapshots/` - raw console blocks of selected samples\n");
        out.append("- `os/` - thread dumps taken during the run\n");
        out.append("- `summary.json` - the numbers above, machine readable\n\n");
        out.append("JVM flags:\n\n```text\n").append(String.join("\n", flags)).append("\n```\n");
        write(file, out);
    }

    /**
     * Renders a table row, flattening newlines so the markdown stays a table.
     *
     * @param out   builder
     * @param key   left column
     * @param value right column
     */
    static void row(final StringBuilder out, final String key, final String value) {
        out.append("| ").append(key).append(" | ").append(value.replace('|', '/')).append(" |\n");
    }

    /**
     * Formats a number with three decimals, or {@code n/a}.
     *
     * @param value value
     * @return the text
     */
    static String decimal(final double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.3f", value) : "n/a";
    }

    /**
     * Formats milliseconds.
     *
     * @param value milliseconds
     * @return the text
     */
    static String millis(final double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.3f ms", value) : "n/a";
    }

    /**
     * Formats seconds.
     *
     * @param value seconds
     * @return the text
     */
    static String seconds(final double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.3f s", value) : "n/a";
    }

    /**
     * Formats a share.
     *
     * @param value share in {@code [0, 1]}
     * @return the text
     */
    static String percent(final double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.2f%%", value * 100.0d) : "n/a";
    }

    private static String number(final double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.4f", value) : "";
    }

    private static String escape(final String value) {
        return value.indexOf(',') >= 0 || value.indexOf('"') >= 0
            ? '"' + value.replace("\"", "\"\"") + '"'
            : value;
    }

    private static void write(final Path file, final CharSequence content) throws IOException {
        final Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    /**
     * Returns the sample file of a run.
     *
     * @param runDir run directory
     * @return {@code ticks.csv}
     */
    static Path ticks(final Path runDir) {
        return runDir.resolve("ticks.csv");
    }

    /**
     * Returns the fund file of a run.
     *
     * @param runDir run directory
     * @return {@code fund.csv}
     */
    static Path fund(final Path runDir) {
        return runDir.resolve("fund.csv");
    }

    /**
     * Returns the summary file of a run.
     *
     * @param runDir run directory
     * @return {@code summary.json}
     */
    static Path summary(final Path runDir) {
        return runDir.resolve("summary.json");
    }

    /**
     * Returns the report file of a run.
     *
     * @param runDir run directory
     * @return {@code analysis.md}
     */
    static Path report(final Path runDir) {
        return runDir.resolve("analysis.md");
    }

    /**
     * Returns the manifest file of a run.
     *
     * @param runDir run directory
     * @return {@code manifest.json}
     */
    static Path manifest(final Path runDir) {
        return runDir.resolve("manifest.json");
    }
}
