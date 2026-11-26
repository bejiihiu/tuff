package kz.bejiihiu.tuff.threads.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

/**
 * The {@code thread-management} section of {@code config/tuff.yml}.
 *
 * <p>One instance owns one read of the file. The section is startup only: the runtime bakes its pools
 * and its sampling policy in at {@code start()}, so a changed value would be a promise the manager cannot
 * keep. {@link #load()} therefore returns a plain result instead of publishing a live snapshot, and the
 * diff layer reports every key of this section as restart-only.
 *
 * <p>A missing file is not an error. The thread layer starts before the compute engine, which is what
 * creates the file from the bundled template, so the first boot legitimately has nothing to read. In
 * that case the observe-only defaults are used and the result says why.
 */
public final class ThreadsConfig {

    /**
     * Name of the configuration file inside the configuration directory.
     */
    public static final String FILE_NAME = "tuff.yml";

    /**
     * Directory that holds all server configuration files, shared with the rest of the server.
     */
    public static final String DIRECTORY_NAME = "config";

    private final Path file;

    /**
     * Creates a configuration bound to a file. Nothing is read until {@link #load()} runs.
     *
     * @param file path of the configuration file
     */
    public ThreadsConfig(final Path file) {
        this.file = Objects.requireNonNull(file, "file");
    }

    /**
     * Creates a configuration for {@code config/tuff.yml} relative to the working directory, which is
     * the server root.
     *
     * @return the server wide configuration
     */
    public static ThreadsConfig serverWide() {
        return new ThreadsConfig(Path.of(DIRECTORY_NAME, FILE_NAME));
    }

    /**
     * Returns the file this configuration reads.
     *
     * @return absolute path of the configuration file
     */
    public Path file() {
        return this.file.toAbsolutePath();
    }

    /**
     * Reads the section and returns the settings with everything that was wrong with them.
     *
     * <p>Never throws for bad input: an unreadable or invalid file becomes a problem plus the defaults,
     * exactly like a single unreadable key. The caller decides how loud that is.
     *
     * @return the settings and the problems found while reading them, never {@code null}
     */
    public ThreadsLoadResult load() {
        final ThreadManagementSettings defaults = ThreadManagementSettings.defaults();
        final List<ConfigProblem> problems = new ArrayList<>();
        if (Files.notExists(this.file)) {
            problems.add(new ConfigProblem(
                this.file.toString(),
                "file does not exist yet, using the observe-only defaults"
            ));
            return new ThreadsLoadResult(defaults, problems, true);
        }
        final YamlConfiguration configuration = new YamlConfiguration();
        try (Reader reader = Files.newBufferedReader(this.file, StandardCharsets.UTF_8)) {
            configuration.load(reader);
        } catch (final IOException ex) {
            problems.add(new ConfigProblem(this.file.toString(), "could not be read (" + ex + "), using defaults"));
            return new ThreadsLoadResult(defaults, problems, false);
        } catch (final InvalidConfigurationException ex) {
            problems.add(new ConfigProblem(this.file.toString(), "is not valid YAML (" + ex.getMessage() + "), using defaults"));
            return new ThreadsLoadResult(defaults, problems, false);
        } catch (final RuntimeException ex) {
            problems.add(new ConfigProblem(this.file.toString(), "could not be parsed (" + ex + "), using defaults"));
            return new ThreadsLoadResult(defaults, problems, false);
        }
        try {
            return new ThreadsLoadResult(ThreadsConfigParser.parse(configuration, problems), problems, false);
        } catch (final RuntimeException ex) {
            // our own bug rather than a hostile file: keep the server alive and say so
            problems.add(new ConfigProblem(ThreadsConfigParser.SECTION, "rejected (" + ex + "), using defaults"));
            return new ThreadsLoadResult(defaults, problems, false);
        }
    }

    /**
     * What one read of the section produced.
     *
     * <p>The settings are always usable: a section that could not be read is replaced by the defaults,
     * and the problem list says what was ignored.
     *
     * @param settings    the parsed settings, or the defaults when nothing usable was found
     * @param problems    everything that was missing, wrong or ignored
     * @param missingFile whether the file did not exist yet; the compute engine creates it on first boot
     */
    public record ThreadsLoadResult(
        ThreadManagementSettings settings,
        List<ConfigProblem> problems,
        boolean missingFile
    ) {

        /**
         * Validates and freezes the result.
         *
         * @param settings    the parsed settings, or the defaults when nothing usable was found
         * @param problems    everything that was missing, wrong or ignored
         * @param missingFile whether the file did not exist yet; the compute engine creates it on first boot
         * @throws NullPointerException if a reference is {@code null}
         */
        public ThreadsLoadResult {
            Objects.requireNonNull(settings, "settings");
            problems = List.copyOf(problems);
        }

        /**
         * Checks whether anything was reported.
         *
         * @return {@code true} if the problem list is not empty
         */
        public boolean hasProblems() {
            return !this.problems.isEmpty();
        }
    }
}
