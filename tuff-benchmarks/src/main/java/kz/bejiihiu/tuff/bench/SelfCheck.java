package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks the pure parts of the harness without starting a server.
 *
 * <p>Parsing, statistics and guardrails decide what a report says, so a defect there would be invisible
 * in a successful run: the numbers would simply be wrong. This command exercises them with known
 * inputs and fixed expectations, and is the cheapest check to run after a change to the analyzer.
 *
 * <p>It is a plain Java check on purpose: the benchmark module has no test framework dependency, and
 * adding one would put a test library on the harness classpath for no gain.
 */
final class SelfCheck {

    private SelfCheck() {
    }

    /**
     * Runs every check.
     *
     * @return {@code 0} when every check passed, {@code 2} otherwise
     */
    static int run() {
        int failures = 0;
        failures += check("json round trip", SelfCheck::jsonRoundTrip);
        failures += check("json rejects garbage", SelfCheck::jsonRejectsGarbage);
        failures += check("statistics", SelfCheck::statistics);
        failures += check("bootstrap is deterministic", SelfCheck::bootstrapDeterministic);
        failures += check("guardrails", SelfCheck::guardrails);
        failures += check("sample parser", SelfCheck::sampleParser);
        if (failures == 0) {
            System.out.println("self-check: ok");
            return 0;
        }
        System.err.println("self-check: " + failures + " check(s) failed");
        return 2;
    }

    private interface Check {

        void run() throws Exception;
    }

    private static int check(final String name, final Check check) {
        try {
            check.run();
            System.out.println("ok   " + name);
            return 0;
        } catch (final AssertionError failure) {
            System.err.println("FAIL " + name + ": " + failure.getMessage());
            return 1;
        } catch (final Exception failure) {
            System.err.println("FAIL " + name + ": " + failure);
            return 1;
        }
    }

    private static void jsonRoundTrip() throws IOException {
        final Map<String, Object> source = new LinkedHashMap<>();
        source.put("text", "quote \" backslash \\ newline \n tab \t");
        source.put("number", 42);
        source.put("fraction", 0.125d);
        source.put("unknown", Double.NaN);
        source.put("flag", true);
        source.put("list", List.of("a", 1, false));
        source.put("nested", Map.of("k", "v"));
        final Path file = Files.createTempFile("tuff-bench-json", ".json");
        try {
            Json.write(file, source);
            @SuppressWarnings("unchecked")
            final Map<String, Object> back = (Map<String, Object>) Json.read(file);
            require(source.get("text").equals(back.get("text")), "string changed: " + back.get("text"));
            require(Json.integer(back, "number", -1L) == 42L, "integer changed");
            require(Json.number(back, "fraction", -1.0d) == 0.125d, "fraction changed");
            require(Double.isNaN(Json.number(back, "unknown", Double.NaN)), "NaN must come back as NaN");
            require(Json.bool(back, "flag", false), "boolean changed");
            require(back.get("list") instanceof List<?> list && list.size() == 3, "list changed");
            require(back.get("nested") instanceof Map<?, ?> nested && "v".equals(nested.get("k")), "map changed");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static void jsonRejectsGarbage() {
        for (final String broken : List.of("{", "{\"a\":}", "[1,]", "nul", "{\"a\":1}x")) {
            try {
                Json.parse(broken);
                throw new AssertionError("accepted malformed document: " + broken);
            } catch (final IllegalArgumentException expected) {
                // the only acceptable answer
            }
        }
    }

    private static void statistics() {
        final double[] values = {1.0d, 2.0d, 3.0d, 4.0d};
        require(Stats.mean(values) == 2.5d, "mean");
        require(Stats.median(values) == 2.5d, "median");
        require(Math.abs(Stats.median(new double[] {5.0d, 1.0d, 3.0d}) - 3.0d) < 1e-9, "odd median");
        require(Math.abs(Stats.percentile(values, 0.5d) - 2.0d) < 1e-9, "p50 nearest rank");
        require(Stats.percentile(values, 1.0d) == 4.0d, "max percentile");
        require(Math.abs(Stats.standardDeviation(values) - 1.290_994_448_735_763_8d) < 1e-9, "sample sd");
        require(Double.isNaN(Stats.mean(new double[0])), "empty mean must be NaN");
    }

    private static void bootstrapDeterministic() {
        final double[] left = {1.0d, 2.0d, 3.0d, 4.0d, 5.0d};
        final double[] right = {0.5d, 1.0d, 1.5d, 2.0d, 2.5d};
        final Stats.Interval first = Stats.bootstrapDifference(left, right, 2_000, 7L);
        final Stats.Interval second = Stats.bootstrapDifference(left, right, 2_000, 7L);
        require(first.equals(second), "same seed must produce the same interval");
        require(first.finite() && first.low() > 0.0d, "a clear positive difference must exclude zero");
        require(Math.abs(first.mean() - 1.5d) < 1e-9, "point estimate");
    }

    private static void guardrails() throws IOException {
        final Path clean = Files.createTempFile("tuff-bench-clean", ".log");
        final Path dirty = Files.createTempFile("tuff-bench-dirty", ".log");
        try {
            Files.writeString(clean, """
                [12:00:00 INFO]: Done (3.1s)! For help, type "help"
                [12:00:01 INFO]: [Tuff/Kairos] Kairos: fixed fund of 2 tick thread(s)
                """, StandardCharsets.UTF_8);
            Files.writeString(dirty, """
                [12:00:00 INFO]: Done (3.1s)! For help, type "help"
                [12:00:10 ERROR]: [FoliaWatchdogThread] Global region has not responded in 5.2008377s
                """, StandardCharsets.UTF_8);
            require(Guardrails.inspect(clean, 0).clean(), "a clean log must be clean");
            final Guardrails.Result dirtyResult = Guardrails.inspect(dirty, 0);
            require(dirtyResult.reasons().contains("watchdog_dump"), "a watchdog line must be a dump");
            require(dirtyResult.evidence().get("watchdog_dump").contains("has not responded"), "evidence line");
            require(Guardrails.inspect(clean, 1).reasons().contains("exit_code=1"), "non-zero exit is a failure");
        } finally {
            Files.deleteIfExists(clean);
            Files.deleteIfExists(dirty);
        }
    }

    private static void sampleParser() throws IOException {
        final Path log = Files.createTempFile("tuff-bench-samples", ".log");
        try {
            Files.writeString(log, """
                [12:00:00 INFO]: [Server] BENCH-SAMPLE 0 warmup
                [12:00:00 INFO]: [Server] regions 2 / avg MSPT 1.2 ms / min TPS 19.8 / lag 0% (0 of 2)
                [12:00:00 INFO]: [Server] ticks: 5s max 4.1 ms, 15s avg 1.0 ms max 6.0 ms; entities 60 / players 0 / chunks 338
                [12:00:00 INFO]: [Server] fund: min 2, live 3, target 3, max 4; elasticity on, ceiling source: test
                [12:00:00 INFO]: [Server] queue: 0 queued, 2 awaiting, 1 executing; tick wait avg 0.0 ms, max 0.0 ms; overdue 0 (threshold 100 ms)
                [12:00:01 INFO]: [Server] BENCH-SAMPLE 1 measure
                [12:00:01 INFO]: [Server] regions 2 / avg MSPT unknown / min TPS unknown / lag 0% (0 of 2)
                """, StandardCharsets.UTF_8);
            final List<SampleParser.SampleMark> marks = List.of(
                new SampleParser.SampleMark(0L, "warmup", 1_000L, 0L),
                new SampleParser.SampleMark(1L, "measure", 2_000L, 1_000L)
            );
            final List<Sample> samples = SampleParser.parse(log, marks).samples();
            require(samples.size() == 2, "two marks must yield two samples");
            final Sample first = samples.getFirst();
            require(first.region().regions() == 2, "region count");
            require(Math.abs(first.region().averageMspt5s() - 1.2d) < 1e-9, "average mspt");
            require(Math.abs(first.region().maximumMspt5s() - 4.1d) < 1e-9, "worst tick");
            require(first.region().entities() == 60L, "entities");
            require(first.fund().present() && first.fund().live() == 3, "fund live");
            require(first.fund().executing() == 1, "fund executing");
            require(Double.isNaN(samples.get(1).region().averageMspt5s()), "unknown must stay NaN");
        } finally {
            Files.deleteIfExists(log);
        }
    }

    private static void require(final boolean condition, final String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
