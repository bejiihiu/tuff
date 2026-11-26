package kz.bejiihiu.tuff.compute.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every {@code thread-management.*} key the shipped template documents is restart only.
 *
 * <p>The thread layer bakes its pool set and its sampling policy in at {@code start()}; nothing in it
 * has a reconfigure call. A key that the diff classified as live would therefore be reported as
 * "applied" while the running manager ignored it, which is worse than no reload at all. The keys are
 * read from the template an administrator actually gets, so a new documented key cannot quietly fall
 * outside the rule.
 *
 * <p>The classification itself is one prefix in {@link ConfigDiff}; this test walks the real key list
 * instead of trusting that the prefix is still there and still spelled right.
 */
@Timeout(120)
class ThreadManagementRestartKeysTest {

    private static final String RESOURCE = "tuff/tuff.yml";
    private static final String SECTION = "thread-management";

    @Test
    void everyDocumentedThreadManagementKeyIsRestartOnly() throws IOException {
        final List<String> keys = documentedKeys();
        assertFalse(keys.isEmpty(), "the template documents the thread-management section");

        final List<String> live = new ArrayList<>();
        for (final String key : keys) {
            final ConfigDiff diff = ConfigDiff.between(Map.of(key, "before"), Map.of(key, "after"));
            assertEquals(List.of(key), diff.changedPaths(), key);
            if (!diff.restartRequiredPaths().contains(key)) {
                live.add(key);
            }
        }

        assertTrue(
            live.isEmpty(),
            () -> "these thread-management keys would be applied to a running layer:\n" + String.join("\n", live)
        );
    }

    @Test
    void theClassifierIsNotBlanketRestartOnly() {
        // a control: compute consumer policies are the live path, so the rule above is a real choice
        final ConfigDiff diff = ConfigDiff.between(
            Map.of("async-compute.consumers.default.max-queued-tasks", "-1"),
            Map.of("async-compute.consumers.default.max-queued-tasks", "64")
        );

        assertTrue(diff.restartRequiredPaths().isEmpty());
        assertEquals(List.of("async-compute.consumers.default.max-queued-tasks"), diff.livePaths());
    }

    /**
     * Reads every leaf key of the thread section from the bundled template.
     *
     * @return dotted keys of {@code thread-management} values, sorted
     * @throws IOException              if the resource cannot be read
     * @throws IllegalStateException    if the bundled template is not valid YAML
     */
    private static List<String> documentedKeys() throws IOException {
        final YamlConfiguration configuration = new YamlConfiguration();
        try (InputStream stream = ThreadManagementRestartKeysTest.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            Assumptions.assumeTrue(stream != null, "bundled configuration template is not on the classpath");
            try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
                configuration.load(reader);
            } catch (final InvalidConfigurationException ex) {
                throw new IllegalStateException("the bundled template is not valid YAML", ex);
            }
        }
        final List<String> keys = new ArrayList<>();
        for (final String key : configuration.getKeys(true)) {
            if (key.startsWith(SECTION + ".") && !configuration.isConfigurationSection(key)) {
                keys.add(key);
            }
        }
        keys.sort(null);
        return keys;
    }
}
