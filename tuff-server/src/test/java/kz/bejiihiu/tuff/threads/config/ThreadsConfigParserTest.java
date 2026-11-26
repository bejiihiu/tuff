package kz.bejiihiu.tuff.threads.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadsConfigParserTest {

    @Test
    void templateParsesCleanlyAndMatchesTheDocumentedDefaults() throws IOException, InvalidConfigurationException {
        final Parsed parsed = parse(template());

        assertTrue(parsed.problems().isEmpty(), () -> "template must parse cleanly, got " + parsed.problems());
        assertEquals(ThreadManagementSettings.defaults(), parsed.settings());
        assertTrue(parsed.settings().enabled());
        assertFalse(parsed.settings().anyManagementEnabled());
    }

    @Test
    void everyKeyMapsOneToOne() {
        final Parsed parsed = parse("""
            thread-management:
              enabled: false
              resources:
                cpu-aware: false
                reserve-cores: 3
                max-concurrent-tasks: 12
              lifecycle:
                centralized-thread-factory: false
                monitor-threads: false
              region-execution:
                managed: true
                preserve-folia-scheduling: true
                scheduler: work-stealing
                threads: 6
                grid-exponent: 2
                kairos-unstable: false
              async-compute:
                managed: true
                max-threads: 4
              execution-pools:
                network:
                  managed: true
                  preserve-specialization: true
                chunk-system:
                  managed: true
                  preserve-specialization: true
                compute:
                  managed: true
                  preserve-specialization: true
              metrics:
                enabled: false
                per-pool: false
                per-thread: false
            """);

        assertTrue(parsed.problems().isEmpty(), () -> "unexpected problems: " + parsed.problems());
        final ThreadManagementSettings settings = parsed.settings();
        assertFalse(settings.enabled());
        assertEquals(new ThreadManagementSettings.Resources(false, 3, 12), settings.resources());
        assertEquals(new ThreadManagementSettings.Lifecycle(false, false), settings.lifecycle());
        assertEquals(
            new ThreadManagementSettings.RegionExecution(
                true, true, ThreadManagementSettings.RegionScheduler.WORK_STEALING, 6, 2, false
            ),
            settings.regionExecution()
        );
        assertEquals(new ThreadManagementSettings.AsyncCompute(true, 4), settings.asyncCompute());
        assertEquals(
            new ThreadManagementSettings.Pools(
                new ThreadManagementSettings.PoolSettings(true, true),
                new ThreadManagementSettings.PoolSettings(true, true),
                new ThreadManagementSettings.PoolSettings(true, true)
            ),
            settings.executionPools()
        );
        assertEquals(new ThreadManagementSettings.Metrics(false, false, false), settings.metrics());
        assertTrue(settings.anyManagementEnabled());
        assertTrue(settings.computeManaged());
    }

    @Test
    void theComputePoolKeyIsTheSameSwitchAsTheAsyncComputeAlias() {
        final Parsed poolKey = parse("""
            thread-management:
              execution-pools:
                compute:
                  managed: true
            """);
        assertTrue(poolKey.problems().isEmpty(), () -> "unexpected problems: " + poolKey.problems());
        assertTrue(poolKey.settings().computeManaged());
        assertFalse(poolKey.settings().asyncCompute().managed());
        assertTrue(poolKey.settings().anyManagementEnabled());

        final Parsed alias = parse("""
            thread-management:
              async-compute:
                managed: true
            """);
        assertTrue(alias.settings().computeManaged(), "the older key must keep working");
        assertFalse(alias.settings().executionPools().compute().managed());
        assertTrue(alias.settings().anyManagementEnabled());

        final Parsed neither = parse("""
            thread-management:
              enabled: true
            """);
        assertFalse(neither.settings().computeManaged());
        assertFalse(neither.settings().anyManagementEnabled());
    }

    @Test
    void anUnreadableKeyFallsBackToItsDefaultAndIsReported() throws IOException, InvalidConfigurationException {
        final YamlConfiguration configuration = template();
        configuration.set("thread-management.enabled", "maybe");
        configuration.set("thread-management.region-execution.scheduler", "nonsense");

        final List<ConfigProblem> problems = new ArrayList<>();
        final ThreadManagementSettings settings = ThreadsConfigParser.parse(configuration, problems);

        assertTrue(settings.enabled(), "an unreadable switch must keep its default");
        assertEquals(
            ThreadManagementSettings.RegionScheduler.EDF,
            settings.regionExecution().scheduler(),
            "an unknown scheduler must keep the default variant"
        );
        assertEquals(2, problems.size(), () -> "expected two problems, got " + problems);
        assertTrue(problems.stream().anyMatch(problem -> problem.key().equals("thread-management.enabled")));
        assertTrue(problems.stream().anyMatch(problem -> problem.key().equals("thread-management.region-execution.scheduler")));
    }

    @Test
    void aContradictorySectionFallsBackToDefaultsInsteadOfHalfSurviving() throws IOException, InvalidConfigurationException {
        final YamlConfiguration configuration = template();
        configuration.set("thread-management.region-execution.threads", 0);

        final List<ConfigProblem> problems = new ArrayList<>();
        final ThreadManagementSettings settings = ThreadsConfigParser.parse(configuration, problems);

        assertEquals(ThreadManagementSettings.RegionExecution.defaults(), settings.regionExecution());
        assertEquals(1, problems.size(), () -> "expected one problem, got " + problems);
        assertEquals("thread-management.region-execution", problems.getFirst().key());
    }

    @Test
    void disablingFoliaSchedulingIsRejectedRatherThanHonoured() throws IOException, InvalidConfigurationException {
        final YamlConfiguration configuration = template();
        configuration.set("thread-management.region-execution.preserve-folia-scheduling", false);

        final List<ConfigProblem> problems = new ArrayList<>();
        final ThreadManagementSettings settings = ThreadsConfigParser.parse(configuration, problems);

        assertTrue(settings.regionExecution().preserveFoliaScheduling());
        assertEquals(1, problems.size(), () -> "expected one problem, got " + problems);
        assertTrue(problems.getFirst().detail().contains("must stay true"), problems.getFirst().detail());
    }

    @Test
    void unknownKeysAreReportedWithTheirFullPath() throws IOException, InvalidConfigurationException {
        final YamlConfiguration configuration = template();
        configuration.set("thread-management.mystery", true);
        configuration.set("thread-management.metrics.sparkles", true);

        final List<ConfigProblem> problems = new ArrayList<>();
        ThreadsConfigParser.parse(configuration, problems);

        assertTrue(problems.stream().anyMatch(problem -> problem.key().equals("thread-management.mystery")));
        assertTrue(problems.stream().anyMatch(problem -> problem.key().equals("thread-management.metrics.sparkles")));
    }

    @Test
    void aMissingSectionUsesTheObserveOnlyDefaults() {
        final List<ConfigProblem> problems = new ArrayList<>();
        final ThreadManagementSettings settings = ThreadsConfigParser.parse(new YamlConfiguration(), problems);

        assertEquals(ThreadManagementSettings.defaults(), settings);
        assertEquals(1, problems.size());
        assertEquals(ThreadsConfigParser.SECTION, problems.getFirst().key());
    }

    @Test
    void flattenCoversEveryKeyOfTheSection() throws IOException, InvalidConfigurationException {
        final Map<String, String> flat = ThreadsConfigParser.flatten(parse(template()).settings());

        assertEquals(23, flat.size(), () -> "unexpected key count: " + flat.keySet());
        assertEquals("true", flat.get("thread-management.enabled"));
        assertEquals("edf", flat.get("thread-management.region-execution.scheduler"));
        assertEquals("-1", flat.get("thread-management.region-execution.threads"));
        assertEquals("true", flat.get("thread-management.execution-pools.network.preserve-specialization"));
        assertEquals("false", flat.get("thread-management.execution-pools.compute.managed"));
    }

    private static YamlConfiguration template() throws IOException, InvalidConfigurationException {
        try (InputStream stream = ThreadsConfigParserTest.class.getClassLoader().getResourceAsStream("tuff/tuff.yml")) {
            assertNotNull(stream, "the shipped template must be on the test classpath");
            try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
                final YamlConfiguration configuration = new YamlConfiguration();
                configuration.load(reader);
                return configuration;
            }
        }
    }

    private static Parsed parse(final String yaml) {
        final YamlConfiguration configuration = new YamlConfiguration();
        try {
            configuration.loadFromString(yaml);
        } catch (final InvalidConfigurationException ex) {
            throw new IllegalArgumentException("the test YAML must be valid", ex);
        }
        return parse(configuration);
    }

    private static Parsed parse(final YamlConfiguration configuration) {
        final List<ConfigProblem> problems = new ArrayList<>();
        final ThreadManagementSettings settings = ThreadsConfigParser.parse(configuration, problems);
        return new Parsed(settings, problems);
    }

    private record Parsed(ThreadManagementSettings settings, List<ConfigProblem> problems) {
    }
}
