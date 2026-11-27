package kz.bejiihiu.tuff.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Parsed command line of the stand.
 *
 * <p>Every knob has a safe default so a short command still produces a complete, valid run: the arm
 * defaults to the EDF baseline, the profile to {@code steady} and the windows to the policy values
 * (ten minutes of warm-up, thirty minutes of measurement). Durations are seconds on the command line
 * and milliseconds inside the artifacts.
 *
 * <p>Only the fields the run actually needs are mutable through the CLI; anything that could silently
 * change two factors at once (for example a second scheduler key) is derived from the arm.
 */
final class BenchOptions {

    /** What the process was asked to do. */
    enum Command {
        RUN,
        SUITE,
        ANALYZE,
        PREPARE,
        SELF_CHECK,
        HELP
    }

    Command command = Command.HELP;
    Path repoRoot = Path.of("").toAbsolutePath();
    Path jar;
    Path outRoot = Path.of("run", "bench");
    Arm arm = Arm.A;
    Arm baseline = Arm.A;
    LoadProfile profile = LoadProfile.STEADY;
    List<Arm> suiteArms = List.of(Arm.A, Arm.A2, Arm.B, Arm.C);
    int threads = 2;
    int maxThreads = 6;
    int warmupSeconds = 600;
    int measureSeconds = 1_800;
    int sampleMillis = 5_000;
    int port = 25_670;
    String heap = "2G";
    int areas = 4;
    int areaChunks = 9;
    int areaStep = 1_000;
    int entitiesPerArea = 600;
    String entityType = "villager";
    int blocks = 10;
    int bootstrapSamples = 10_000;
    long seed = 1_337L;
    double marginHardMiss = -1.0d;
    double marginRatio = 0.02d;
    String benchId;
    String runId;
    boolean force;
    boolean rebuildTemplate;
    List<String> extraJvm = List.of();
    Path analyzeDir;

    private BenchOptions() {
    }

    /**
     * Returns the load shape described by the options.
     *
     * @return the spec written into the manifest
     */
    LoadSpec loadSpec() {
        return new LoadSpec(this.areas, this.areaChunks, this.areaStep, this.entitiesPerArea, this.entityType);
    }

    /**
     * Parses the command line.
     *
     * @param args raw arguments, the first one being the command
     * @return the parsed options
     * @throws IllegalArgumentException on an unknown command, unknown flag or malformed value
     */
    static BenchOptions parse(final String[] args) {
        final BenchOptions options = new BenchOptions();
        if (args.length == 0) {
            options.command = Command.HELP;
            return options;
        }
        options.command = switch (args[0].toLowerCase(Locale.ROOT)) {
            case "run" -> Command.RUN;
            case "suite" -> Command.SUITE;
            case "analyze" -> Command.ANALYZE;
            case "prepare" -> Command.PREPARE;
            case "self-check" -> Command.SELF_CHECK;
            case "help", "--help", "-h" -> Command.HELP;
            default -> throw new IllegalArgumentException(
                "unknown command '" + args[0] + "'; expected run, suite, analyze, prepare, self-check or help"
            );
        };
        for (int index = 1; index < args.length; ++index) {
            final String flag = args[index];
            switch (flag) {
                case "--jar" -> {
                    options.jar = Path.of(value(args, ++index, flag));
                }
                case "--out" -> {
                    options.outRoot = Path.of(value(args, ++index, flag));
                }
                case "--dir" -> {
                    options.analyzeDir = Path.of(value(args, ++index, flag));
                }
                case "--arm" -> {
                    options.arm = Arm.parse(value(args, ++index, flag));
                }
                case "--baseline" -> {
                    options.baseline = Arm.parse(value(args, ++index, flag));
                }
                case "--arms" -> {
                    options.suiteArms = List.of(value(args, ++index, flag).split(","))
                        .stream()
                        .map(Arm::parse)
                        .toList();
                }
                case "--profile" -> {
                    options.profile = LoadProfile.parse(value(args, ++index, flag));
                }
                case "--threads" -> {
                    options.threads = integer(args, ++index, flag);
                }
                case "--max-threads" -> {
                    options.maxThreads = integer(args, ++index, flag);
                }
                case "--warmup" -> {
                    options.warmupSeconds = integer(args, ++index, flag);
                }
                case "--measure" -> {
                    options.measureSeconds = integer(args, ++index, flag);
                }
                case "--sample" -> {
                    options.sampleMillis = integer(args, ++index, flag);
                }
                case "--port" -> {
                    options.port = integer(args, ++index, flag);
                }
                case "--heap" -> {
                    options.heap = value(args, ++index, flag);
                }
                case "--areas" -> {
                    options.areas = integer(args, ++index, flag);
                }
                case "--area-chunks" -> {
                    options.areaChunks = integer(args, ++index, flag);
                }
                case "--area-step" -> {
                    options.areaStep = integer(args, ++index, flag);
                }
                case "--entities" -> {
                    options.entitiesPerArea = integer(args, ++index, flag);
                }
                case "--entity" -> {
                    options.entityType = value(args, ++index, flag);
                }
                case "--blocks" -> {
                    options.blocks = integer(args, ++index, flag);
                }
                case "--bootstrap" -> {
                    options.bootstrapSamples = integer(args, ++index, flag);
                }
                case "--seed" -> {
                    options.seed = Long.parseLong(value(args, ++index, flag));
                }
                case "--margin-hard-miss" -> {
                    options.marginHardMiss = Double.parseDouble(value(args, ++index, flag));
                }
                case "--margin-ratio" -> {
                    options.marginRatio = Double.parseDouble(value(args, ++index, flag));
                }
                case "--bench-id" -> {
                    options.benchId = value(args, ++index, flag);
                }
                case "--run-id" -> {
                    options.runId = value(args, ++index, flag);
                }
                case "--force" -> {
                    options.force = true;
                }
                case "--rebuild" -> {
                    options.rebuildTemplate = true;
                }
                case "--jvm" -> {
                    options.extraJvm = List.of(value(args, ++index, flag).split("\\s+"));
                }
                default -> throw new IllegalArgumentException("unknown flag '" + flag + "'");
            }
        }
        options.validate();
        return options;
    }

    private void validate() {
        if (this.warmupSeconds < 0) {
            throw new IllegalArgumentException("--warmup must not be negative");
        }
        if (this.measureSeconds < 1) {
            throw new IllegalArgumentException("--measure must be at least one second");
        }
        if (this.sampleMillis < 200) {
            throw new IllegalArgumentException("--sample must be at least 200 ms");
        }
        if (this.threads < 1) {
            throw new IllegalArgumentException("--threads must be positive");
        }
        if (this.arm.elastic() && this.maxThreads <= this.threads) {
            throw new IllegalArgumentException(
                "arm C needs --max-threads above --threads; without headroom it is arm B with a different label"
            );
        }
        if (this.blocks < 1) {
            throw new IllegalArgumentException("--blocks must be positive");
        }
        if (this.bootstrapSamples < 100) {
            throw new IllegalArgumentException("--bootstrap must be at least 100");
        }
        if (this.command == Command.RUN || this.command == Command.SUITE) {
            if (this.jar == null) {
                throw new IllegalArgumentException("--jar is required: point it at the built paperclip jar");
            }
        }
    }

    private static String value(final String[] args, final int index, final String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("flag " + flag + " needs a value");
        }
        return args[index];
    }

    private static int integer(final String[] args, final int index, final String flag) {
        return Integer.parseInt(value(args, index, flag));
    }

    /**
     * Returns the per-run JVM flags.
     *
     * <p>GC logging and the JFR recording are part of the artifact contract, so they are added here and
     * cannot be removed by an operator: a run without them would look complete and be unanalyzable.
     *
     * @param runDir      run directory, which receives {@code gc.log} and {@code jfr/recording.jfr}
     * @param extraFlags  operator supplied flags
     * @return the full flag list, without the java executable and the jar
     */
    static List<String> jvmFlags(final Path runDir, final String heap, final List<String> extraFlags) {
        try {
            Files.createDirectories(runDir.resolve("jfr"));
        } catch (final java.io.IOException failure) {
            throw new IllegalStateException("cannot create the JFR directory under " + runDir, failure);
        }
        final java.util.ArrayList<String> flags = new java.util.ArrayList<>();
        flags.add("-Xms" + heap);
        flags.add("-Xmx" + heap);
        flags.add("-XX:+UseG1GC");
        flags.add("-XX:+AlwaysPreTouch");
        flags.add("-Xlog:gc*:file=" + runDir.resolve("gc.log").toAbsolutePath() + ":time,uptime,level,tags");
        flags.add("-XX:StartFlightRecording=filename="
            + runDir.resolve("jfr").resolve("recording.jfr").toAbsolutePath()
            + ",settings=profile,dumponexit=true");
        flags.add("-Dfile.encoding=UTF-8");
        flags.addAll(extraFlags);
        return List.copyOf(flags);
    }

    /**
     * Returns the java executable of the JVM that runs the harness.
     *
     * <p>Gradle already selected a JDK 25 toolchain for this process, so the harness reuses it instead
     * of guessing a path; the same JDK for every arm is part of the validity rules.
     *
     * @return absolute path to {@code java} or {@code java.exe}
     */
    static Path javaExecutable() {
        final String executable = System.getProperty("os.name", "")
            .toLowerCase(Locale.ROOT)
            .contains("win") ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable).toAbsolutePath();
    }
}
