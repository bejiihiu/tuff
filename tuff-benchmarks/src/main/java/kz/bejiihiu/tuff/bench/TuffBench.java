package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Command line entry point of the scheduler evidence stand.
 *
 * <p>Exit codes are part of the contract: {@code 0} means the requested work completed and every run
 * it produced is valid, {@code 2} means the harness could not do the work at all, {@code 3} means the
 * work finished but at least one run is invalid (a guardrail fired or an artifact is missing). A CI job
 * can therefore treat {@code 3} as "the stand ran, do not trust the numbers".
 */
public final class TuffBench {

    private TuffBench() {
    }

    /**
     * Runs the command line.
     *
     * @param args first argument is the command: {@code run}, {@code suite}, {@code analyze} or {@code prepare}
     */
    public static void main(final String[] args) {
        final BenchOptions options;
        try {
            options = BenchOptions.parse(args);
        } catch (final IllegalArgumentException failure) {
            System.err.println("tuff-bench: " + failure.getMessage());
            usage();
            System.exit(2);
            return;
        }
        try {
            final int code = switch (options.command) {
                case HELP -> {
                    usage();
                    yield 0;
                }
                case PREPARE -> prepare(options);
                case RUN -> runOne(options);
                case SUITE -> runSuite(options);
                case ANALYZE -> analyze(options);
                case SELF_CHECK -> SelfCheck.run();
            };
            System.exit(code);
        } catch (final IOException failure) {
            System.err.println("tuff-bench: " + failure.getMessage());
            System.exit(2);
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            System.err.println("tuff-bench: interrupted");
            System.exit(2);
        }
    }

    private static int prepare(final BenchOptions options) throws IOException, InterruptedException {
        options.repoRoot = repoRoot(options.repoRoot);
        final Identity identity = Identity.probe(options.repoRoot, options.jar, options.profile, options.benchId);
        final Path template = TemplateBuilder.ensure(options, identity);
        System.out.println("template: " + template);
        System.out.println("jar sha256: " + identity.jarSha());
        System.out.println("patch set: " + identity.patchDigest() + " (" + identity.patchFiles() + " files)");
        return 0;
    }

    private static int runOne(final BenchOptions options) throws IOException, InterruptedException {
        options.repoRoot = repoRoot(options.repoRoot);
        final Identity identity = Identity.probe(options.repoRoot, options.jar, options.profile, options.benchId);
        final Path template = TemplateBuilder.ensure(options, identity);
        final Path benchDir = options.outRoot.toAbsolutePath().resolve(identity.benchId());
        final String runId = options.runId == null || options.runId.isBlank()
            ? String.format(Locale.ROOT, "%s-%s-%02d", options.arm.label(), options.profile.cli(), 1)
            : options.runId;
        final RunSummary summary = new RunExecutor(options, identity, benchDir, template, options.arm)
            .execute(runId, options.port);
        System.out.println("run: " + benchDir.resolve("runs").resolve(runId));
        System.out.println("report: " + Artifacts.report(benchDir.resolve("runs").resolve(runId)));
        System.out.println("hard miss: " + Artifacts.percent(summary.hardMissShare())
            + ", valid: " + summary.valid()
            + (summary.invalidReasons().isEmpty() ? "" : ", reasons: " + summary.invalidReasons()));
        return summary.valid() ? 0 : 3;
    }

    private static int runSuite(final BenchOptions options) throws IOException, InterruptedException {
        options.repoRoot = repoRoot(options.repoRoot);
        final Identity identity = Identity.probe(options.repoRoot, options.jar, options.profile, options.benchId);
        final Path template = TemplateBuilder.ensure(options, identity);
        final Analysis.Result result = SuiteRunner.run(options, identity, template);
        System.out.println("report: " + result.report());
        System.out.println("table: " + result.table());
        result.gates().forEach((gate, verdict) -> System.out.println("gate " + gate + ": " + verdict));
        return result.invalidRuns() == 0 ? 0 : 3;
    }

    private static int analyze(final BenchOptions options) throws IOException {
        final Path dir = options.analyzeDir == null ? options.outRoot.toAbsolutePath() : options.analyzeDir;
        final Analysis.Result result = Analysis.run(dir, options);
        System.out.println("report: " + result.report());
        System.out.println("table: " + result.table());
        System.out.println("valid runs: " + result.validRuns() + ", invalid: " + result.invalidRuns());
        result.gates().forEach((gate, verdict) -> System.out.println("gate " + gate + ": " + verdict));
        return result.invalidRuns() == 0 ? 0 : 3;
    }

    private static Path repoRoot(final Path start) throws IOException {
        Path current = start.toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isDirectory(current.resolve(".git"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IOException("not inside a git checkout: " + start);
    }

    private static void usage() {
        System.out.println("""
            tuff-bench - scheduler evidence stand

            usage:
              tuff-bench run     --jar <paperclip.jar> [options]
              tuff-bench suite   --jar <paperclip.jar> [--blocks 10] [options]
              tuff-bench analyze --dir <block dir> [options]
              tuff-bench prepare --jar <paperclip.jar>
              tuff-bench self-check

            options:
              --arm A|A2|B|C          arm of a single run (default A)
              --arms A,A2,B,C         arms of a block
              --baseline A            baseline arm for non-inferiority (default A)
              --profile <name>        steady, churn, burst, idle-compute (default steady)
              --threads N             region thread floor (default 2)
              --max-threads N         Kairos ceiling for arm C (default 6)
              --warmup S              warm-up seconds (default 600)
              --measure S             measurement seconds (default 1800)
              --sample MS             console sample interval (default 5000)
              --areas N               force-loaded areas, 1..4 (default 4)
              --area-chunks N         side of one area in chunks (default 9)
              --area-step N           distance between areas in chunks (default 1000)
              --entities N            entities per area (default 600)
              --entity <id>           entity type (default villager)
              --heap 2G               -Xms and -Xmx of the server
              --out <dir>             artifact root (default run/bench)
              --bench-id <id>         block identifier
              --run-id <id>           run identifier
              --port N                first server port (default 25670)
              --blocks N              runs per arm in a suite (default 10)
              --bootstrap N           bootstrap resamples (default 10000)
              --seed N                order and bootstrap seed (default 1337)
              --margin-hard-miss M    absolute margin for hard miss; default 1% of baseline
              --margin-ratio R        relative margin for other metrics (default 0.02)
              --force                 redo finished runs
              --jvm "<flags>"         extra JVM flags

            exit codes: 0 ok, 2 harness failure, 3 at least one invalid run
            """);
    }
}
