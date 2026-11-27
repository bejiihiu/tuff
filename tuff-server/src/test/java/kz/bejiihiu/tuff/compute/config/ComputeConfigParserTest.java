package kz.bejiihiu.tuff.compute.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputeConfigParserTest {

    @Test
    void templateMatchesShippedDefaultsExceptDocumentedPlanConsumers() throws IOException {
        final Parsed parsed = parse(template());

        final ComputeSettings defaults = TuffComputeConfig.defaultSettings();
        assertTrue(parsed.problems().isEmpty(), () -> "template must parse cleanly, got " + parsed.problems());

        assertEquals(defaults.enabled(), parsed.settings().enabled());
        assertEquals(defaults.workers(), parsed.settings().workers());
        assertEquals(defaults.queue(), parsed.settings().queue());
        assertEquals(defaults.scheduling(), parsed.settings().scheduling());
        assertEquals(defaults.metrics(), parsed.settings().metrics());
        assertEquals(defaults.adaptive(), parsed.settings().adaptive());
        assertEquals(defaults.lanes(), parsed.settings().lanes());
        assertEquals(defaults.shutdownGracePeriod(), parsed.settings().shutdownGracePeriod());

        assertEquals(defaults.consumers().defaultPolicy(), parsed.settings().consumers().defaultPolicy());
        assertEquals(defaults.consumers().allowDynamicRegistration(), parsed.settings().consumers().allowDynamicRegistration());
        assertEquals(
            Set.of("chunk-generation", "pathfinding", "ai-computation"),
            parsed.settings().consumers().overrides().keySet()
        );
    }

    @Test
    void planConsumersUseRelativeLimits() throws IOException {
        final ComputeSettings settings = parse(template()).settings();

        assertRelative(0.60d, settings, "chunk-generation");
        assertRelative(0.30d, settings, "pathfinding");
        assertRelative(0.20d, settings, "ai-computation");
        for (final String id : List.of("chunk-generation", "pathfinding", "ai-computation")) {
            final ConsumerPolicy policy = settings.consumers().overrides().get(id);
            assertTrue(policy.enabled(), id + " must be enabled");
            assertEquals(1.0d, policy.fairShareWeight(), 0.0d);
            assertEquals(-1, policy.maxQueuedTasks());
            assertEquals(OverflowPolicy.REJECT, policy.overflowPolicy());
            assertEquals(Duration.ZERO, policy.maxQueueWait());
        }
    }

    @Test
    void everyKeyMapsOneToOne() {
        final Parsed parsed = parse("""
            async-compute:
              enabled: false
              workers:
                threads: 6
                min-threads: 3
                max-threads: 8
                keep-alive-seconds: 45
                thread-name-prefix: "Custom Worker"
              queue:
                capacity: 1024
                overflow-policy: drop-oldest
              scheduling:
                strategy: priority
                work-stealing: true
                priority-aging: false
                aging-interval-millis: 250
                max-aging-boost: 5
                idle-wait-millis: 25
              consumers:
                allow-dynamic-registration: false
                default:
                  enabled: true
                  max-concurrent: 2
                  fair-share-weight: 1.5
                  max-queued-tasks: 64
                  overflow-policy: drop-oldest
                  max-queue-wait-seconds: 30
                chunk-generation:
                  enabled: false
                  max-concurrent: 0.25
                  fair-share-weight: 3.0
                  max-queued-tasks: 128
                  overflow-policy: reject
                  max-queue-wait-seconds: 5
              metrics:
                enabled: false
                jfr: true
                throughput-window-seconds: 20
                sample-cpu: false
                sample-interval-seconds: 2
              adaptive:
                enabled: true
                min-concurrency: 2
                max-concurrency: 6
                sample-window-seconds: 15
                min-samples: 30
                increase-step: 0.2
                decrease-factor: 0.4
                target-utilization: 0.8
              lanes:
                virtual:
                  enabled: false
                  low-priority: false
                  normal-spill: false
                  max-concurrent: 16
                  spill-wait-millis: 0
                  spill-queue-depth: 0
              shutdown-grace-seconds: 7
            """);

        assertTrue(parsed.problems().isEmpty(), () -> "unexpected problems " + parsed.problems());
        final ComputeSettings settings = parsed.settings();

        assertFalse(settings.enabled());
        assertEquals(new ComputeSettings.Workers(6, 3, 8, Duration.ofSeconds(45), "Custom Worker"), settings.workers());
        assertEquals(new ComputeSettings.QueueSettings(1024, OverflowPolicy.DROP_OLDEST), settings.queue());
        assertEquals(
            new ComputeSettings.Scheduling(
                ComputeSettings.Scheduling.Strategy.PRIORITY,
                true,
                false,
                Duration.ofMillis(250),
                5,
                Duration.ofMillis(25)
            ),
            settings.scheduling()
        );

        final ComputeSettings.Consumers consumers = settings.consumers();
        assertFalse(consumers.allowDynamicRegistration());
        assertEquals(
            new ConsumerPolicy(
                true,
                ConsumerPolicy.ConcurrencyLimit.absolute(2),
                64,
                1.5d,
                OverflowPolicy.DROP_OLDEST,
                Duration.ofSeconds(30)
            ),
            consumers.defaultPolicy()
        );
        assertEquals(
            new ConsumerPolicy(
                false,
                ConsumerPolicy.ConcurrencyLimit.relative(0.25d),
                128,
                3.0d,
                OverflowPolicy.REJECT,
                Duration.ofSeconds(5)
            ),
            consumers.overrides().get("chunk-generation")
        );

        assertEquals(
            new ComputeSettings.Metrics(false, true, Duration.ofSeconds(20), false, Duration.ofSeconds(2)),
            settings.metrics()
        );
        assertEquals(
            new ComputeSettings.Adaptive(true, 2, 6, Duration.ofSeconds(15), 30, 0.2d, 0.4d, 0.8d),
            settings.adaptive()
        );
        assertEquals(
            new ComputeSettings.Lanes(
                new ComputeSettings.Lanes.Virtual(false, false, false, 16, Duration.ZERO, 0)
            ),
            settings.lanes()
        );
        assertEquals(Duration.ofSeconds(7), settings.shutdownGracePeriod());
    }

    @Test
    void missingSectionUsesDefaults() {
        final Parsed parsed = parse("""
            async-compute:
              workers:
                threads: 4
            """);

        assertTrue(parsed.problems().isEmpty(), () -> "unexpected problems " + parsed.problems());
        final ComputeSettings defaults = TuffComputeConfig.defaultSettings();
        assertEquals(4, parsed.settings().workers().threads());
        assertEquals(defaults.queue(), parsed.settings().queue());
        assertEquals(defaults.scheduling(), parsed.settings().scheduling());
        assertEquals(defaults.consumers(), parsed.settings().consumers());
        assertEquals(defaults.metrics(), parsed.settings().metrics());
        assertEquals(defaults.adaptive(), parsed.settings().adaptive());
        assertEquals(defaults.lanes(), parsed.settings().lanes());
        assertEquals(defaults.shutdownGracePeriod(), parsed.settings().shutdownGracePeriod());
    }

    @Test
    void emptyFileFallsBackToDefaults() {
        final Parsed parsed = parse("");

        assertEquals(TuffComputeConfig.defaultSettings(), parsed.settings());
        assertProblem(parsed.problems(), "async-compute", "missing");
    }

    @Test
    void brokenValuesFallBackPerKeyAndPerSection() {
        final Parsed parsed = parse("""
            async-compute:
              enabled: "maybe"
              workers:
                threads: 0
                min-threads: 0
                keep-alive-seconds: -5
              queue:
                capacity: 0
                overflow-policy: explode
              scheduling:
                strategy: fastest
                aging-interval-millis: 0
                max-aging-boost: 0
              consumers:
                default:
                  max-concurrent: 2.5
                  fair-share-weight: 0
                  max-queued-tasks: -7
                  overflow-policy: nope
                  max-queue-wait-seconds: -1
                chunk-generation: 7
              metrics:
                jfr: "on"
                throughput-window-seconds: 0
              adaptive:
                min-concurrency: 0
                increase-step: 2.0
              shutdown-grace-seconds: -1
            """);

        assertEquals(TuffComputeConfig.defaultSettings(), parsed.settings());
        assertProblem(parsed.problems(), "async-compute.enabled", "true or false");
        assertProblem(parsed.problems(), "async-compute.workers", "rejected");
        assertProblem(parsed.problems(), "async-compute.workers.keep-alive-seconds", "positive");
        assertProblem(parsed.problems(), "async-compute.queue", "rejected");
        assertProblem(parsed.problems(), "async-compute.queue.overflow-policy", "reject or drop-oldest");
        assertProblem(parsed.problems(), "async-compute.scheduling", "rejected");
        assertProblem(parsed.problems(), "async-compute.scheduling.strategy", "weighted-fair");
        assertProblem(parsed.problems(), "async-compute.scheduling.aging-interval-millis", "positive");
        assertProblem(parsed.problems(), "async-compute.consumers.default", "inherited policy");
        assertProblem(parsed.problems(), "async-compute.consumers.default.max-concurrent", "(0.0, 1.0]");
        assertProblem(parsed.problems(), "async-compute.consumers.default.max-queue-wait-seconds", "must not be negative");
        assertProblem(parsed.problems(), "async-compute.consumers.chunk-generation", "expected a section");
        assertProblem(parsed.problems(), "async-compute.metrics.jfr", "true or false");
        assertProblem(parsed.problems(), "async-compute.metrics.throughput-window-seconds", "positive");
        assertProblem(parsed.problems(), "async-compute.adaptive", "rejected");
        assertProblem(parsed.problems(), "async-compute.shutdown-grace-seconds", "must not be negative");
    }

    @Test
    void unknownKeysAreReportedAndIgnored() {
        final Parsed parsed = parse("""
            other-section:
              whatever: 1
            async-compute:
              surprise: 1
              workers:
                threads: 2
                turbo: true
              consumers:
                mystery-consumer:
                  enabled: true
                  turbo: 2
              metrics:
                flame-graphs: true
              lanes:
                extra: 1
                virtual:
                  turbo: true
            """);

        assertProblem(parsed.problems(), "other-section", "unknown key");
        assertProblem(parsed.problems(), "async-compute.surprise", "unknown key");
        assertProblem(parsed.problems(), "async-compute.workers.turbo", "unknown key");
        assertProblem(parsed.problems(), "async-compute.consumers.mystery-consumer.turbo", "unknown key");
        assertProblem(parsed.problems(), "async-compute.metrics.flame-graphs", "unknown key");
        assertProblem(parsed.problems(), "async-compute.lanes.extra", "unknown key");
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.turbo", "unknown key");

        assertEquals(2, parsed.settings().workers().threads());
        assertNotNull(parsed.settings().consumers().overrides().get("mystery-consumer"));
    }

    @Test
    void missingLaneSectionsKeepTheDefaults() {
        final Parsed onlyVirtual = parse("""
            async-compute:
              lanes:
                virtual:
                  enabled: false
            """);

        assertTrue(onlyVirtual.problems().isEmpty(), () -> "unexpected problems " + onlyVirtual.problems());
        assertEquals(
            new ComputeSettings.Lanes.Virtual(false, true, true, 64, Duration.ofMillis(50L), 64),
            onlyVirtual.settings().lanes().virtual()
        );

        final Parsed empty = parse("""
            async-compute:
              lanes: {}
            """);

        assertTrue(empty.problems().isEmpty(), () -> "unexpected problems " + empty.problems());
        assertEquals(ComputeSettings.Lanes.defaults(), empty.settings().lanes());
    }

    @Test
    void brokenVirtualLaneValuesFallBackPerKey() {
        final Parsed parsed = parse("""
            async-compute:
              lanes:
                virtual:
                  enabled: "sometimes"
                  low-priority: 2
                  normal-spill: 1
                  max-concurrent: 0
                  spill-wait-millis: -5
                  spill-queue-depth: -1
            """);

        assertEquals(ComputeSettings.Lanes.defaults().virtual(), parsed.settings().lanes().virtual());
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.enabled", "true or false");
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.low-priority", "true or false");
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.normal-spill", "true or false");
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.max-concurrent", "at least 1");
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.spill-wait-millis", "must not be negative");
        assertProblem(parsed.problems(), "async-compute.lanes.virtual.spill-queue-depth", "must not be negative");
    }

    @Test
    void virtualLaneAcceptsZeroOneAndLargeValues() {
        final Parsed minimal = parse("""
            async-compute:
              lanes:
                virtual:
                  max-concurrent: 1
                  spill-wait-millis: 0
                  spill-queue-depth: 0
            """);

        assertTrue(minimal.problems().isEmpty(), () -> "unexpected problems " + minimal.problems());
        assertEquals(
            new ComputeSettings.Lanes.Virtual(true, true, true, 1, Duration.ZERO, 0),
            minimal.settings().lanes().virtual()
        );

        final Parsed large = parse("""
            async-compute:
              lanes:
                virtual:
                  max-concurrent: 2147483647
                  spill-wait-millis: 2147483647
                  spill-queue-depth: 2147483647
            """);

        assertTrue(large.problems().isEmpty(), () -> "unexpected problems " + large.problems());
        assertEquals(
            new ComputeSettings.Lanes.Virtual(
                true,
                true,
                true,
                Integer.MAX_VALUE,
                Duration.ofMillis(Integer.MAX_VALUE),
                Integer.MAX_VALUE
            ),
            large.settings().lanes().virtual()
        );
    }

    @Test
    void maxConcurrentDistinguishesSharesFromCounts() {
        final Parsed parsed = parse("""
            async-compute:
              consumers:
                default:
                  max-concurrent: -1
                chunk-generation:
                  max-concurrent: 0.60
                pathfinding:
                  max-concurrent: 4
                ai-computation:
                  max-concurrent: 1.0
                entity-ticking:
                  max-concurrent: 1
                redstone:
                  max-concurrent: 2.5
                lighting:
                  max-concurrent: 0
            """);

        final ComputeSettings.Consumers consumers = parsed.settings().consumers();
        assertEquals(ConsumerPolicy.ConcurrencyLimit.automatic(), consumers.defaultPolicy().concurrencyLimit());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.relative(0.60d), consumers.overrides().get("chunk-generation").concurrencyLimit());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.absolute(4), consumers.overrides().get("pathfinding").concurrencyLimit());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.relative(1.0d), consumers.overrides().get("ai-computation").concurrencyLimit());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.absolute(1), consumers.overrides().get("entity-ticking").concurrencyLimit());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.automatic(), consumers.overrides().get("redstone").concurrencyLimit());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.automatic(), consumers.overrides().get("lighting").concurrencyLimit());

        assertProblem(parsed.problems(), "async-compute.consumers.redstone.max-concurrent", "(0.0, 1.0]");
        assertProblem(parsed.problems(), "async-compute.consumers.lighting.max-concurrent", "must be -1");
    }

    @Test
    void consumerKeysInheritFromDefaultPolicy() {
        final Parsed parsed = parse("""
            async-compute:
              consumers:
                default:
                  enabled: false
                  max-concurrent: 3
                  fair-share-weight: 2.0
                  max-queued-tasks: 16
                  overflow-policy: drop-oldest
                  max-queue-wait-seconds: 9
                chunk-generation:
                  max-concurrent: 2
            """);

        final ConsumerPolicy inherited = parsed.settings().consumers().defaultPolicy();
        final ConsumerPolicy override = parsed.settings().consumers().overrides().get("chunk-generation");

        assertFalse(override.enabled());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.absolute(2), override.concurrencyLimit());
        assertEquals(2.0d, override.fairShareWeight(), 0.0d);
        assertEquals(16, override.maxQueuedTasks());
        assertEquals(OverflowPolicy.DROP_OLDEST, override.overflowPolicy());
        assertEquals(Duration.ofSeconds(9), override.maxQueueWait());
        assertEquals(inherited.enabled(), override.enabled());
    }

    @Test
    void boundaryValuesAreAcceptedOrReported() {
        final Parsed parsed = parse("""
            async-compute:
              workers:
                threads: 2147483647
                min-threads: 1
                max-threads: -1
                keep-alive-seconds: 0.5
              queue:
                capacity: 2147483647
              consumers:
                default:
                  max-concurrent: 1
                  max-queued-tasks: 0
                  max-queue-wait-seconds: 0
              adaptive:
                min-concurrency: 1
                max-concurrency: 2147483647
            """);

        assertTrue(parsed.problems().isEmpty(), () -> "unexpected problems " + parsed.problems());
        assertEquals(Integer.MAX_VALUE, parsed.settings().workers().threads());
        assertEquals(Duration.ofMillis(500), parsed.settings().workers().keepAlive());
        assertEquals(Integer.MAX_VALUE, parsed.settings().queue().capacity());
        assertEquals(0, parsed.settings().consumers().defaultPolicy().maxQueuedTasks());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.absolute(1), parsed.settings().consumers().defaultPolicy().concurrencyLimit());

        final Parsed overflow = parse("""
            async-compute:
              queue:
                capacity: 2147483648
            """);
        assertEquals(8192, overflow.settings().queue().capacity());
        assertProblem(overflow.problems(), "async-compute.queue.capacity", "out of range");
    }

    @Test
    void quotedNumbersStillParse() {
        final Parsed parsed = parse("""
            async-compute:
              workers:
                threads: "4"
              metrics:
                sample-interval-seconds: "2.5"
            """);

        assertTrue(parsed.problems().isEmpty(), () -> "unexpected problems " + parsed.problems());
        assertEquals(4, parsed.settings().workers().threads());
        assertEquals(Duration.ofMillis(2500), parsed.settings().metrics().sampleInterval());
    }

    @Test
    void workerBoundsAreCheckedTogether() {
        final Parsed parsed = parse("""
            async-compute:
              workers:
                threads: 8
                min-threads: 2
                max-threads: 4
            """);

        assertEquals(TuffComputeConfig.defaultSettings().workers(), parsed.settings().workers());
        assertProblem(parsed.problems(), "async-compute.workers", "must not exceed maxThreads");
    }

    @Test
    void automaticSizingIsClampedByBudget() throws IOException {
        final ComputeSettings settings = parse(template()).settings();

        assertEquals(7, settings.workers().resolveThreadCount(8));
        assertEquals(2, settings.workers().resolveThreadCount(1));
        assertThrows(IllegalArgumentException.class, () -> settings.workers().resolveThreadCount(0));
    }

    @Test
    void adaptiveMaxConcurrencyMinusOneMeansTheWholePool() {
        final Parsed minusOne = parse("""
            async-compute:
              adaptive:
                max-concurrency: -1
            """);

        assertTrue(minusOne.problems().isEmpty(), () -> "unexpected problems " + minusOne.problems());
        assertEquals(-1, minusOne.settings().adaptive().maxConcurrency());
    }

    @Test
    void adaptiveMaxConcurrencyBelowTheFloorIsReportedAtItsKey() {
        for (final int broken : new int[] {-5, 0}) {
            final Parsed parsed = parse("""
                async-compute:
                  adaptive:
                    max-concurrency: %d
                """.formatted(broken));

            assertEquals(-1, parsed.settings().adaptive().maxConcurrency(),
                "the default of -1 must stay in effect for " + broken);
            assertProblem(parsed.problems(), "async-compute.adaptive.max-concurrency", "must be -1");
        }
    }

    @Test
    void theReliefSectionParsesItsOwnKeysAndKeepsTheDefaultsOff() {
        final ParsedRelief missing = parseRelief("""
            async-compute:
              enabled: true
            """);
        assertTrue(missing.problems().isEmpty(), () -> "unexpected problems " + missing.problems());
        assertEquals(RegionReliefSettings.defaults(), missing.settings());
        assertEquals(0, missing.settings().effectiveReservedCores(),
            "the trigger is off by default, so no core is held back from tick pressure");

        final ParsedRelief parsed = parseRelief("""
            async-compute:
              relief:
                enabled: true
                max-in-flight-per-region: 3
                reserved-cores: 2
                consumer:
                  enabled: true
                  max-concurrent: 0.5
                  fair-share-weight: 2.0
                  max-queued-tasks: 32
                  overflow-policy: reject
                  max-queue-wait-seconds: 4
            """);

        assertTrue(parsed.problems().isEmpty(), () -> "unexpected problems " + parsed.problems());
        final RegionReliefSettings settings = parsed.settings();
        assertTrue(settings.enabled());
        assertEquals(3, settings.maxInFlightPerRegion());
        assertEquals(2, settings.reservedCores());
        assertEquals(2, settings.effectiveReservedCores());
        assertEquals(ConsumerPolicy.ConcurrencyLimit.relative(0.5d), settings.consumerPolicy().concurrencyLimit());
        assertEquals(2.0d, settings.consumerPolicy().fairShareWeight(), 0.0d);
        assertEquals(32, settings.consumerPolicy().maxQueuedTasks());
        assertEquals(Duration.ofSeconds(4L), settings.consumerPolicy().maxQueueWait());
    }

    @Test
    void aDisabledReliefPathHoldsNoCoreBackFromPressure() {
        final RegionReliefSettings settings = new RegionReliefSettings(
            false, 8, 3, RegionReliefSettings.defaultConsumerPolicy()
        );
        assertEquals(0, settings.effectiveReservedCores(), "a path that is off must not take a core from the pool");
    }

    @Test
    void brokenReliefValuesFallBackPerKey() {
        final ParsedRelief parsed = parseRelief("""
            async-compute:
              relief:
                enabled: "sometimes"
                max-in-flight-per-region: 0
                reserved-cores: -2
                consumer:
                  max-concurrent: 0
                  turbo: true
            """);

        final RegionReliefSettings settings = parsed.settings();
        assertEquals(RegionReliefSettings.defaults().enabled(), settings.enabled());
        assertEquals(RegionReliefSettings.defaults().maxInFlightPerRegion(), settings.maxInFlightPerRegion());
        assertEquals(RegionReliefSettings.defaults().reservedCores(), settings.reservedCores());
        assertProblem(parsed.problems(), "async-compute.relief.enabled", "true or false");
        assertProblem(parsed.problems(), "async-compute.relief.max-in-flight-per-region", "at least 1");
        assertProblem(parsed.problems(), "async-compute.relief.reserved-cores", "must not be negative");
        assertProblem(parsed.problems(), "async-compute.relief.consumer.max-concurrent", "must be -1");
        assertProblem(parsed.problems(), "async-compute.relief.consumer.turbo", "unknown key");
    }

    @Test
    void everyReliefKeyAppearsInTheFlattenedViewAndIsRestartOnly() {
        final RegionReliefSettings settings = new RegionReliefSettings(
            true, 5, 1, RegionReliefSettings.defaultConsumerPolicy()
        );
        final Map<String, String> flat = ComputeConfigParser.flattenRelief(settings);

        assertEquals("true", flat.get("async-compute.relief.enabled"));
        assertEquals("5", flat.get("async-compute.relief.max-in-flight-per-region"));
        assertEquals("1", flat.get("async-compute.relief.reserved-cores"));
        assertTrue(flat.containsKey("async-compute.relief.consumer.max-concurrent"), flat.toString());
        assertTrue(flat.containsKey("async-compute.relief.consumer.max-queue-wait-seconds"), flat.toString());
    }

    private static ParsedRelief parseRelief(final String yaml) {
        final List<ConfigProblem> problems = new ArrayList<>();
        final YamlConfiguration configuration = new YamlConfiguration();
        try {
            configuration.loadFromString(yaml);
        } catch (final InvalidConfigurationException ex) {
            throw new AssertionError("test yaml is broken", ex);
        }
        final ComputeSettings parsed = ComputeConfigParser.parse(configuration, problems);
        final RegionReliefSettings relief = ComputeConfigParser.parseRelief(configuration, problems);
        return new ParsedRelief(parsed, relief, List.copyOf(problems));
    }

    private record ParsedRelief(ComputeSettings compute, RegionReliefSettings settings, List<ConfigProblem> problems) {
    }

    private static void assertRelative(final double fraction, final ComputeSettings settings, final String consumerId) {
        final ConsumerPolicy policy = settings.consumers().overrides().get(consumerId);
        assertNotNull(policy, consumerId + " must be configured");
        assertEquals(ConsumerPolicy.ConcurrencyLimit.relative(fraction), policy.concurrencyLimit());
    }

    private static void assertProblem(final List<ConfigProblem> problems, final String key, final String fragment) {
        for (final ConfigProblem problem : problems) {
            if (problem.key().equals(key) && problem.detail().contains(fragment)) {
                return;
            }
        }
        throw new AssertionError("no problem for '" + key + "' containing '" + fragment + "', got " + problems);
    }

    static String template() throws IOException {
        try (InputStream stream = ComputeConfigParser.class.getClassLoader().getResourceAsStream(ConfigTemplate.RESOURCE)) {
            assertNotNull(stream, "bundled template " + ConfigTemplate.RESOURCE + " is missing from the classpath");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Parsed parse(final String yaml) {
        final List<ConfigProblem> problems = new ArrayList<>();
        final YamlConfiguration configuration = new YamlConfiguration();
        try {
            configuration.loadFromString(yaml);
        } catch (final InvalidConfigurationException ex) {
            throw new AssertionError("test yaml is broken", ex);
        }
        return new Parsed(ComputeConfigParser.parse(configuration, problems), List.copyOf(problems));
    }

    private record Parsed(ComputeSettings settings, List<ConfigProblem> problems) {
    }
}
