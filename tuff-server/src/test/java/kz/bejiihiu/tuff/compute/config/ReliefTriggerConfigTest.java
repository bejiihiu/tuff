package kz.bejiihiu.tuff.compute.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The overrun trigger keys of {@code config/tuff.yml}: defaults, one to one mapping, fallbacks and the
 * restart inherited from the relief block.
 */
@Timeout(30)
class ReliefTriggerConfigTest {

    private static final String TRIGGER = "async-compute.relief.trigger";

    @Test
    void anEmptyFileGivesTheQuietDefaults() {
        final Parsed parsed = parse("""
            async-compute: {}
            """);

        final RegionReliefSettings.Trigger defaults = RegionReliefSettings.Trigger.defaults();
        assertFalse(parsed.trigger().enabled(), "the trigger must be off by default");
        assertEquals(defaults, parsed.trigger());
        assertTrue(parsed.problems().isEmpty(), () -> "defaults must not report problems, got " + parsed.problems());
    }

    @Test
    void everyKeyMapsToOneField() {
        final Parsed parsed = parse("""
            async-compute:
              relief:
                trigger:
                  enabled: true
                  overrun-millis: 55
                  enter-overruns: 4
                  exit-calm-ticks: 9
                  cooldown-seconds: 12
                  max-windows: 3
                  rate-window-seconds: 90
                  stale-after-seconds: 7
            """);

        final RegionReliefSettings.Trigger trigger = parsed.trigger();
        assertTrue(trigger.enabled());
        assertEquals(Duration.ofMillis(55L), trigger.overrun());
        assertEquals(4, trigger.enterOverruns());
        assertEquals(9, trigger.exitCalm());
        assertEquals(Duration.ofSeconds(12L), trigger.cooldown());
        assertEquals(3, trigger.maxWindows());
        assertEquals(Duration.ofSeconds(90L), trigger.rateWindow());
        assertEquals(Duration.ofSeconds(7L), trigger.staleAfter());
        assertTrue(parsed.problems().isEmpty(), parsed.problems().toString());
    }

    @Test
    void everyBrokenValueFallsBackToItsDefaultAndIsReported() {
        final Parsed parsed = parse("""
            async-compute:
              relief:
                trigger:
                  overrun-millis: 0
                  enter-overruns: 0
                  exit-calm-ticks: -2
                  cooldown-seconds: -1
                  max-windows: 0
                  rate-window-seconds: 0
                  stale-after-seconds: -5
            """);

        final RegionReliefSettings.Trigger defaults = RegionReliefSettings.Trigger.defaults();
        assertEquals(defaults.overrun(), parsed.trigger().overrun());
        assertEquals(defaults.enterOverruns(), parsed.trigger().enterOverruns());
        assertEquals(defaults.exitCalm(), parsed.trigger().exitCalm());
        assertEquals(defaults.cooldown(), parsed.trigger().cooldown());
        assertEquals(defaults.maxWindows(), parsed.trigger().maxWindows());
        assertEquals(defaults.rateWindow(), parsed.trigger().rateWindow());
        assertEquals(defaults.staleAfter(), parsed.trigger().staleAfter());
        assertProblem(parsed.problems(), TRIGGER + ".overrun-millis");
        assertProblem(parsed.problems(), TRIGGER + ".enter-overruns");
        assertProblem(parsed.problems(), TRIGGER + ".exit-calm-ticks");
        assertProblem(parsed.problems(), TRIGGER + ".cooldown-seconds");
        assertProblem(parsed.problems(), TRIGGER + ".max-windows");
        assertProblem(parsed.problems(), TRIGGER + ".rate-window-seconds");
        assertProblem(parsed.problems(), TRIGGER + ".stale-after-seconds");
    }

    @Test
    void anUnknownTriggerKeyIsReportedAndIgnored() {
        final Parsed parsed = parse("""
            async-compute:
              relief:
                trigger:
                  enter-overruns: 2
                  enter-ticks: 2
            """);

        assertEquals(2, parsed.trigger().enterOverruns());
        assertProblem(parsed.problems(), TRIGGER + ".enter-ticks");
    }

    @Test
    void aWrongTriggerTypeIsReportedAndTheDefaultsStay() {
        final Parsed parsed = parse("""
            async-compute:
              relief:
                trigger: 5
            """);

        assertEquals(RegionReliefSettings.Trigger.defaults(), parsed.trigger());
        assertProblem(parsed.problems(), TRIGGER);
    }

    @Test
    void flatteningCarriesEveryTriggerKey() {
        final RegionReliefSettings.Trigger trigger = new RegionReliefSettings.Trigger(
            true,
            Duration.ofMillis(45L),
            2,
            4,
            Duration.ofSeconds(5L),
            1,
            Duration.ofSeconds(60L),
            Duration.ofSeconds(3L)
        );

        final Map<String, String> flat = ComputeConfigParser.flattenTrigger(trigger);

        assertEquals("true", flat.get(TRIGGER + ".enabled"));
        assertEquals("45", flat.get(TRIGGER + ".overrun-millis"));
        assertEquals("2", flat.get(TRIGGER + ".enter-overruns"));
        assertEquals("4", flat.get(TRIGGER + ".exit-calm-ticks"));
        assertEquals("5", flat.get(TRIGGER + ".cooldown-seconds"));
        assertEquals("1", flat.get(TRIGGER + ".max-windows"));
        assertEquals("60", flat.get(TRIGGER + ".rate-window-seconds"));
        assertEquals("3", flat.get(TRIGGER + ".stale-after-seconds"));
        assertEquals(8, flat.size());
    }

    @Test
    void aTriggerChangeNeedsARestartLikeTheRestOfRelief() {
        final RegionReliefSettings.Trigger defaults = RegionReliefSettings.Trigger.defaults();
        final RegionReliefSettings.Trigger changed = new RegionReliefSettings.Trigger(
            true,
            defaults.overrun(),
            defaults.enterOverruns(),
            defaults.exitCalm(),
            defaults.cooldown(),
            defaults.maxWindows(),
            defaults.rateWindow(),
            defaults.staleAfter()
        );

        final ConfigDiff diff = ConfigDiff.between(
            ComputeConfigParser.flattenTrigger(defaults),
            ComputeConfigParser.flattenTrigger(changed)
        );

        assertEquals(List.of(TRIGGER + ".enabled"), diff.changedPaths());
        assertEquals(List.of(TRIGGER + ".enabled"), diff.restartRequiredPaths());
        assertTrue(diff.livePaths().isEmpty());
    }

    @Test
    void theShippedTemplateCarriesTheTriggerOffWithTheDocumentedNumbers() throws Exception {
        final List<ConfigProblem> problems = new ArrayList<>();
        final YamlConfiguration configuration = new YamlConfiguration();
        configuration.loadFromString(ComputeConfigParserTest.template());

        final RegionReliefSettings.Trigger trigger = ComputeConfigParser.parseTrigger(configuration, problems);

        assertEquals(RegionReliefSettings.Trigger.defaults(), trigger);
        assertTrue(problems.isEmpty(), problems.toString());
    }

    private static void assertProblem(final List<ConfigProblem> problems, final String key) {
        for (final ConfigProblem problem : problems) {
            if (problem.key().equals(key)) {
                return;
            }
        }
        throw new AssertionError("no problem for '" + key + "', got " + problems);
    }

    private static Parsed parse(final String yaml) {
        final List<ConfigProblem> problems = new ArrayList<>();
        final YamlConfiguration configuration = new YamlConfiguration();
        try {
            configuration.loadFromString(yaml);
        } catch (final InvalidConfigurationException ex) {
            throw new AssertionError("test yaml is broken", ex);
        }
        // unknown-key reporting lives in parse(), so the trigger section is validated the same way a
        // real file is: wrong keys and wrong types both land in the same problem list
        ComputeConfigParser.parse(configuration, problems);
        final RegionReliefSettings.Trigger trigger = ComputeConfigParser.parseTrigger(configuration, problems);
        return new Parsed(trigger, List.copyOf(problems));
    }

    private record Parsed(RegionReliefSettings.Trigger trigger, List<ConfigProblem> problems) {
    }
}
