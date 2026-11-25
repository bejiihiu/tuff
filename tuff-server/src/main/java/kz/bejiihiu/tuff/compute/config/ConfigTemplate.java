package kz.bejiihiu.tuff.compute.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.bukkit.configuration.file.YamlConfiguration;

/**
 * Access to the shipped {@code tuff.yml} template and the fallback writer for it.
 *
 * <p>The template lives on the classpath so the comments, ordering and defaults an administrator sees
 * are the exact bytes maintained in the repository. If it is ever missing from a build, a plain file
 * generated from {@link TuffComputeConfig#defaultSettings()} is written instead so the server still
 * starts with a well formed configuration.
 */
final class ConfigTemplate {

    /**
     * Classpath location of the template, relative to the jar root.
     */
    static final String RESOURCE = "tuff/tuff.yml";

    private static final List<String> FALLBACK_HEADER = List.of(
        "Tuff - compute scheduler configuration.",
        "",
        "The bundled template was not found on the classpath, so this file was generated from the",
        "built-in defaults. Values and units match the documentation of the shipped tuff.yml.",
        ""
    );

    private ConfigTemplate() {
    }

    /**
     * Writes the default configuration if the target does not exist yet, creating parent directories.
     *
     * @param file file to create
     * @throws IOException if the template cannot be read or the file cannot be written
     */
    static void writeDefault(final Path file) throws IOException {
        final Path absolute = file.toAbsolutePath();
        final Path parent = absolute.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        final byte[] template = readTemplate();
        if (template.length > 0) {
            Files.write(absolute, template);
            return;
        }
        Files.writeString(absolute, generate(), StandardCharsets.UTF_8);
    }

    /**
     * Reads the bundled template bytes.
     *
     * @return the template, or an empty array if the resource is not on the classpath
     * @throws IOException if the resource cannot be read
     */
    static byte[] readTemplate() throws IOException {
        try (InputStream stream = ConfigTemplate.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (stream == null) {
                return new byte[0];
            }
            return stream.readAllBytes();
        }
    }

    /**
     * Generates a configuration file from the built-in defaults, used only when the template resource
     * is unavailable.
     *
     * @return file contents with a header and the canonical key order
     */
    private static String generate() {
        final YamlConfiguration configuration = new YamlConfiguration();
        for (final Map.Entry<String, String> entry : ComputeConfigParser.flatten(TuffComputeConfig.defaultSettings()).entrySet()) {
            configuration.set(entry.getKey(), scalar(entry.getValue()));
        }

        final StringBuilder out = new StringBuilder();
        for (final String line : FALLBACK_HEADER) {
            if (line.isEmpty()) {
                out.append("#\n");
            } else {
                out.append("# ").append(line).append('\n');
            }
        }
        out.append('\n');
        out.append(configuration.saveToString());
        return out.toString();
    }

    private static Object scalar(final String value) {
        if ("true".equals(value) || "false".equals(value)) {
            return Boolean.valueOf(value);
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException _) {
            // not an int
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException _) {
            // not a long
        }
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException _) {
            // not a double
        }
        return value;
    }
}
