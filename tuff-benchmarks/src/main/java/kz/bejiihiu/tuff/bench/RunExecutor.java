package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes one run of one arm: config, server process, load, samples, artifacts.
 *
 * <p>The order is fixed and intentionally boring: write the arm's config, start the real jar, wait
 * for the server to be ready, apply the profile, sample through the warm-up, sample through the
 * measurement window, stop gracefully, then parse and summarize what the process actually produced.
 *
 * <p>A failure anywhere still leaves a complete directory: the manifest, the log, whatever samples
 * arrived and a summary marked invalid with the reason. A missing directory would look like a run that
 * was never attempted, which is the one thing an evidence stand must not do.
 */
final class RunExecutor {

    private static final Duration BOOT = Duration.ofMinutes(10L);
    private static final Duration STOP = Duration.ofMinutes(2L);
    private static final Pattern REGION_LINE = Pattern.compile("regions (\\d+) / avg MSPT");

    private final BenchOptions options;
    private final Identity identity;
    private final Path benchDir;
    private final Path template;
    private final Path java;
    private final Arm arm;

    RunExecutor(
        final BenchOptions options,
        final Identity identity,
        final Path benchDir,
        final Path template,
        final Arm arm
    ) {
        this.options = options;
        this.identity = identity;
        this.benchDir = benchDir;
        this.template = template;
        this.java = BenchOptions.javaExecutable();
        this.arm = arm;
    }

    /**
     * Runs one arm once.
     *
     * @param runId identifier, unique inside the block
     * @param port  server port, unique inside the block
     * @return the computed summary, valid or not
     * @throws IOException          if the template cannot be copied
     * @throws InterruptedException if the thread is interrupted
     */
    RunSummary execute(final String runId, final int port) throws IOException, InterruptedException {
        final Path runDir = this.benchDir.resolve("runs").resolve(runId);
        TemplateBuilder.deleteTree(runDir);
        ServerConfig.copyTemplate(this.template, runDir);
        ServerConfig.writeServerFiles(runDir, port, "1337");
        ServerConfig.writeTuffYml(runDir, this.arm, this.options.threads, this.options.maxThreads, 4, this.options.kairosJfr);
        ServerConfig.patchPaperGlobal(runDir, this.arm, this.options.threads, 4);

        final List<String> flags = BenchOptions.jvmFlags(runDir, this.options.heap, this.options.extraJvm);
        final Map<String, Object> manifest = new LinkedHashMap<>(this.identity.toMap(this.options));
        manifest.put("run_id", runId);
        manifest.put("arm", this.arm.label());
        manifest.put("scheduler", this.arm.scheduler());
        manifest.put("elastic", this.arm.elastic());
        manifest.put("port", port);
        manifest.put("jvm_flags", flags);
        manifest.put("command", ServerProcess.describeCommand(this.java, flags, Path.of(this.identity.jarPath())));
        manifest.put("start_utc", Instant.now().toString());
        manifest.put("config_sha256", HostEnvironment.configHashes(runDir));
        manifest.put("mode", this.options.measureSeconds < 300 ? "smoke" : "full");
        Json.write(Artifacts.manifest(runDir), manifest);

        final LoadSpec spec = this.options.loadSpec();
        final List<SampleParser.SampleMark> marks = new ArrayList<>(1_024);
        final List<String> extraReasons = new ArrayList<>(2);
        ServerProcess server = null;
        int exitCode = -1;
        double stopSeconds = -1.0d;
        long measureStartEpoch = -1L;
        long measureEndEpoch = -1L;
        try {
            server = ServerProcess.start(this.java, flags, Path.of(this.identity.jarPath()), runDir);
            manifest.put("pid", server.pid());
            if (server.waitFor("Done (", BOOT)) {
                for (final String command : this.options.profile.setup(spec)) {
                    server.send(command);
                }
                this.awaitRegions(server, this.options.areas);
                Thread.sleep(2_000L);
                final long settledNanos = System.nanoTime();
                final long settledEpoch = System.currentTimeMillis();

                long seq = this.phase(server, marks, 0L, "warmup", this.options.warmupSeconds, settledNanos, settledEpoch);
                ThreadDump.capture(
                    Path.of(this.identity.javaHome()),
                    server.pid(),
                    runDir.resolve("os").resolve("threads-warmup-end.txt")
                );
                measureStartEpoch = System.currentTimeMillis();
                seq = this.phase(
                    server, marks, seq, "measure", this.options.measureSeconds, settledNanos, settledEpoch
                );
                measureEndEpoch = System.currentTimeMillis();
                marks.add(this.sample(server, seq, "last", settledNanos, settledEpoch));
                Thread.sleep(1_000L);
                ThreadDump.capture(
                    Path.of(this.identity.javaHome()),
                    server.pid(),
                    runDir.resolve("os").resolve("threads-measure-end.txt")
                );
            } else {
                extraReasons.add("startup_failed");
            }
        } catch (final IOException | RuntimeException failure) {
            extraReasons.add("harness_error: " + failure.getMessage());
        } finally {
            if (server != null) {
                exitCode = server.alive() ? server.stop(STOP) : server.exitCode();
                stopSeconds = server.stopSeconds();
                server.awaitLogEnd(Duration.ofSeconds(20L));
                server.close();
            }
        }

        final Path log = runDir.resolve("server.log");
        if (!Files.isRegularFile(log)) {
            Files.writeString(log, "[bench] the server produced no console output\n", StandardCharsets.UTF_8);
        }
        final SampleParser.Result parsed;
        try {
            parsed = SampleParser.parse(log, marks);
        } catch (final IOException failure) {
            extraReasons.add("log_unreadable: " + failure.getMessage());
            return this.degraded(runDir, manifest, runId, List.of(), flags, failure.getMessage());
        }
        final List<Sample> samples = parsed.samples();
        final long from = measureStartEpoch < 0L ? System.currentTimeMillis() : measureStartEpoch;
        final long to = measureEndEpoch < 0L ? from + 1L : measureEndEpoch;
        final Path recording = runDir.resolve("jfr").resolve("recording.jfr");
        final JfrFacts.Facts jfr = JfrFacts.read(recording, from, to);
        final JfrTicks.Result perTick = JfrTicks.read(recording, JfrTicks.tool(), from, to);
        final boolean perTickRequested = this.arm.kairosOptIn() && this.options.kairosJfr;
        final Guardrails.Result guardrails = Guardrails.inspect(log, exitCode);
        final double measuredSeconds = measureStartEpoch < 0L || measureEndEpoch < 0L
            ? 0.0d
            : (measureEndEpoch - measureStartEpoch) / 1_000.0d;

        final RunSummary summary = RunMetrics.compute(
            this.options,
            this.arm,
            runId,
            this.identity.benchId(),
            this.identity.gitHead(),
            this.identity.jarSha(),
            this.identity.jdk(),
            this.identity.hostTag(),
            samples,
            jfr,
            guardrails,
            exitCode,
            stopSeconds,
            measuredSeconds,
            perTick,
            perTickRequested,
            extraReasons
        );

        Artifacts.writeTicks(Artifacts.ticks(runDir), runId, perTick, from);
        Artifacts.writeFund(Artifacts.fund(runDir), runId, perTick, samples);
        Artifacts.writeSamples(runDir.resolve("samples.csv"), runId, samples);
        Artifacts.writeSnapshots(runDir.resolve("snapshots"), parsed, "warmup");
        Artifacts.writeSnapshots(runDir.resolve("snapshots"), parsed, "measure");
        Json.write(Artifacts.summary(runDir), summary.toMap());
        Artifacts.writeRunReport(Artifacts.report(runDir), summary, jfr, guardrails, flags);

        manifest.put("end_utc", Instant.now().toString());
        manifest.put("measure_start_epoch_ms", measureStartEpoch);
        manifest.put("measure_end_epoch_ms", measureEndEpoch);
        manifest.put("exit_code", exitCode);
        manifest.put("stop_seconds", stopSeconds);
        manifest.put("samples", samples.size());
        manifest.put("valid", summary.valid());
        manifest.put("invalid_reasons", summary.invalidReasons());
        manifest.put("hard_miss_source", summary.hardMissSource());
        manifest.put("hard_miss_share", summary.hardMissShare());
        manifest.put("hard_miss_ticks", summary.hardMissTicks());
        manifest.put("ticks", summary.ticks());
        manifest.put("skipped_periods", summary.skippedPeriods());
        manifest.put("kairos_jfr", this.options.kairosJfr);
        manifest.put("per_tick_requested", perTickRequested);
        manifest.put("fund_passes", perTick.fund().size());
        manifest.put("guardrails", guardrails.evidence());
        Json.write(Artifacts.manifest(runDir), manifest);
        return summary;
    }

    private RunSummary degraded(
        final Path runDir,
        final Map<String, Object> manifest,
        final String runId,
        final List<Sample> samples,
        final List<String> flags,
        final String reason
    ) throws IOException {
        final Guardrails.Result unreadable = new Guardrails.Result(
            List.of("log_unreadable"),
            Map.of("log_unreadable", reason)
        );
        final RunSummary summary = RunMetrics.compute(
            this.options,
            this.arm,
            runId,
            this.identity.benchId(),
            this.identity.gitHead(),
            this.identity.jarSha(),
            this.identity.jdk(),
            this.identity.hostTag(),
            samples,
            JfrFacts.Facts.absent(reason),
            unreadable,
            -1,
            -1.0d,
            0.0d,
            JfrTicks.Result.absent("log unreadable"),
            this.arm.kairosOptIn() && this.options.kairosJfr,
            List.of("log_unreadable: " + reason)
        );
        Artifacts.writeTicks(Artifacts.ticks(runDir), runId, JfrTicks.Result.absent("log unreadable"), -1L);
        Artifacts.writeFund(Artifacts.fund(runDir), runId, JfrTicks.Result.absent("log unreadable"), samples);
        Artifacts.writeSamples(runDir.resolve("samples.csv"), runId, samples);
        Json.write(Artifacts.summary(runDir), summary.toMap());
        Artifacts.writeRunReport(
            Artifacts.report(runDir),
            summary,
            JfrFacts.Facts.absent(reason),
            unreadable,
            flags
        );
        manifest.put("valid", false);
        manifest.put("invalid_reasons", summary.invalidReasons());
        Json.write(Artifacts.manifest(runDir), manifest);
        return summary;
    }

    private long phase(
        final ServerProcess server,
        final List<SampleParser.SampleMark> marks,
        final long firstSeq,
        final String phase,
        final int seconds,
        final long runStartNanos,
        final long runStartEpoch
    ) throws IOException, InterruptedException {
        long seq = firstSeq;
        final long phaseStartMillis = System.currentTimeMillis();
        while (true) {
            final long elapsed = System.currentTimeMillis() - phaseStartMillis;
            if (elapsed >= seconds * 1_000L) {
                return seq;
            }
            for (final String command : this.options.profile.periodic(this.options.loadSpec(), elapsed)) {
                server.send(command);
            }
            marks.add(this.sample(server, seq, phase, runStartNanos, runStartEpoch));
            ++seq;
            final long remaining = seconds * 1_000L - (System.currentTimeMillis() - phaseStartMillis);
            if (remaining > 0L) {
                Thread.sleep(Math.min(remaining, this.options.sampleMillis));
            }
        }
    }

    private SampleParser.SampleMark sample(
        final ServerProcess server,
        final long seq,
        final String phase,
        final long runStartNanos,
        final long runStartEpoch
    ) throws IOException {
        final long epoch = System.currentTimeMillis();
        final long elapsed = (System.nanoTime() - runStartNanos) / 1_000_000L;
        server.send("say " + SampleParser.MARKER + ' ' + seq + ' ' + phase);
        server.send("tuff scheduler");
        server.send("tuff threads");
        return new SampleParser.SampleMark(seq, phase, epoch, elapsed);
    }

    /**
     * Waits, bounded, until the region aggregate reports the planned number of regions.
     *
     * <p>Force-loaded chunks are generated by the chunk system, not by the command, so the first
     * samples can legitimately see fewer regions. Waiting for the count is the honest alternative to
     * measuring during generation.
     *
     * @param server running server
     * @param areas  planned area count
     * @return {@code true} when the count was reached
     */
    private boolean awaitRegions(final ServerProcess server, final int areas) throws IOException {
        final long deadline = System.nanoTime() + Duration.ofMinutes(4L).toNanos();
        while (System.nanoTime() - deadline < 0L) {
            server.send("tuff threads");
            if (server.waitFor(line -> {
                final Matcher matcher = REGION_LINE.matcher(line);
                return matcher.find() && Integer.parseInt(matcher.group(1)) >= areas;
            }, Duration.ofSeconds(2L))) {
                return true;
            }
        }
        return false;
    }

}
