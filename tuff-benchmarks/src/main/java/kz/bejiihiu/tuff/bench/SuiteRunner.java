package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Runs a block of arms in randomized order and analyzes the block afterwards.
 *
 * <p>Blocks are the unit of interleaving: inside one block every arm runs once, in an order drawn from
 * a fixed seed, so a slow drift of the machine hits every arm roughly equally instead of the arm that
 * happened to run last. The plan is written to {@code plan.json} before the first run, which makes it
 * impossible to reorder the arms after seeing the data.
 *
 * <p>An already finished and valid run is skipped, so a block can be resumed; {@code --force} redoes
 * everything.
 */
final class SuiteRunner {

    private SuiteRunner() {
    }

    /**
     * Executes a block.
     *
     * @param options  harness options
     * @param identity block identity
     * @param template prepared server template
     * @return the analysis
     * @throws IOException          if an artifact cannot be written
     * @throws InterruptedException if the thread is interrupted
     */
    static Analysis.Result run(final BenchOptions options, final Identity identity, final Path template)
        throws IOException, InterruptedException {
        final Path benchDir = options.outRoot.toAbsolutePath().resolve(identity.benchId());
        final List<Map<String, Object>> plan = plan(options);
        final Map<String, Object> planDocument = new LinkedHashMap<>();
        planDocument.put("bench_id", identity.benchId());
        planDocument.put("seed", options.seed);
        planDocument.put("blocks", options.blocks);
        planDocument.put("arms", options.suiteArms.stream().map(Arm::label).toList());
        planDocument.put("entries", plan);
        Json.write(benchDir.resolve("plan.json"), planDocument);
        Json.write(benchDir.resolve("env.json"), identity.toMap(options));

        int position = 0;
        for (final Map<String, Object> entry : plan) {
            final String runId = String.valueOf(entry.get("run_id"));
            final int port = ((Number) entry.get("port")).intValue();
            final Path runDir = benchDir.resolve("runs").resolve(runId);
            if (!options.force && isFinished(runDir)) {
                System.out.printf(
                    Locale.ROOT, "[bench] %02d/%02d skip %s (finished)%n", position + 1, plan.size(), runId
                );
                ++position;
                continue;
            }
            System.out.printf(
                Locale.ROOT, "[bench] %02d/%02d run %s (%s, port %d)%n",
                position + 1, plan.size(), runId, entry.get("arm"), port
            );
            final RunSummary summary = new RunExecutor(
                options, identity, benchDir, template, Arm.parse(String.valueOf(entry.get("arm")))
            ).execute(runId, port);
            System.out.printf(
                Locale.ROOT, "[bench]      -> hard miss %s, valid %s%n",
                Artifacts.percent(summary.hardMissShare()), summary.valid()
            );
            ++position;
        }
        return Analysis.run(benchDir, options);
    }

    private static boolean isFinished(final Path runDir) {
        final Path summary = Artifacts.summary(runDir);
        if (!Files.isRegularFile(summary)) {
            return false;
        }
        try {
            final Object parsed = Json.read(summary);
            return parsed instanceof final Map<?, ?> map && Boolean.TRUE.equals(map.get("valid"));
        } catch (final IOException failure) {
            return false;
        }
    }

    private static List<Map<String, Object>> plan(final BenchOptions options) {
        final List<Map<String, Object>> entries = new ArrayList<>();
        final Map<String, Integer> counts = new LinkedHashMap<>();
        final Random random = new Random(options.seed);
        int port = options.port;
        for (int block = 0; block < options.blocks; ++block) {
            final List<Arm> order = new ArrayList<>(options.suiteArms);
            java.util.Collections.shuffle(order, random);
            for (final Arm arm : order) {
                final int ordinal = counts.merge(arm.label(), 1, Integer::sum);
                final Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("block", block);
                entry.put("ordinal", ordinal);
                entry.put("arm", arm.label());
                entry.put("profile", options.profile.cli());
                entry.put("run_id", String.format(
                    Locale.ROOT, "%s-%s-%02d", arm.label(), options.profile.cli(), ordinal
                ));
                entry.put("port", port++);
                entries.add(entry);
            }
        }
        return List.copyOf(entries);
    }
}
