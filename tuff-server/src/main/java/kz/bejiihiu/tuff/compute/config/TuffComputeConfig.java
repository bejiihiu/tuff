package kz.bejiihiu.tuff.compute.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bukkit.configuration.Configuration;
import org.bukkit.configuration.InvalidConfigurationException;
import org.jspecify.annotations.Nullable;

/**
 * The {@code config/tuff.yml} file of the compute scheduler.
 *
 * <p>One instance owns one file. {@link #load()} is meant for startup, {@link #reload()} for the
 * {@code /tuff reload} command: both re-read the file, validate it, publish an immutable
 * {@link ComputeSettings} snapshot and hand the change to an optional {@link ComputeConfigApplyTarget}.
 * Reading is lock free for everybody else - {@link #settings()} and {@link #problems()} return the
 * current snapshot through a volatile field, so a region thread can call them at any time.
 *
 * <p>Broken input never takes the server down: a wrong value falls back to its default, a wrong
 * section to that section's defaults, an unparseable file to the previous snapshot. Every fallback is
 * reported both as a log warning and in {@link ConfigLoadResult#problems()} so a command can show the
 * same list to an administrator.
 *
 * <p>Automatic worker sizing must not use {@code Runtime.availableProcessors()}: the region scheduler,
 * chunk workers and IO threads already own part of the machine. Pass a budget from
 * {@link ComputeThreadBudget} to {@link #resolveThreadCount(int)} instead.
 */
public final class TuffComputeConfig {

    /**
     * Name of the configuration file inside the configuration directory.
     */
    public static final String FILE_NAME = "tuff.yml";

    /**
     * Directory that holds all server configuration files, shared with the rest of the server.
     */
    public static final String DIRECTORY_NAME = "config";

    /**
     * Grace period shipped for a server: the world save starts right after the pool stops and
     * {@code /restart} gives the whole shutdown about ten seconds, so the pool gets a fraction of the
     * standalone default of thirty seconds.
     */
    private static final Duration SERVER_SHUTDOWN_GRACE = Duration.ofSeconds(5);

    private static final Logger LOGGER = LogManager.getLogger("Tuff/ComputeConfig");

    private final Path file;
    private final Object lock = new Object();

    private volatile ConfigLoadResult state;
    private volatile RegionReliefSettings reliefSettings = RegionReliefSettings.defaults();
    private volatile RegionReliefSettings.Trigger triggerSettings = RegionReliefSettings.Trigger.defaults();
    private volatile @Nullable ComputeConfigApplyTarget applyTarget;
    private volatile Map<String, String> flattened = Map.of();
    private boolean loaded;

    /**
     * Creates a configuration bound to a file. Nothing is read until {@link #load()} runs.
     *
     * @param file path of the configuration file
     */
    public TuffComputeConfig(final Path file) {
        this.file = Objects.requireNonNull(file, "file");
        this.state = new ConfigLoadResult(defaultSettings(), List.of(), ConfigDiff.empty(), false, false);
    }

    /**
     * Creates a configuration for {@code config/tuff.yml} relative to the working directory, which is
     * the server root.
     *
     * @return the server wide configuration
     */
    public static TuffComputeConfig serverWide() {
        return new TuffComputeConfig(Path.of(DIRECTORY_NAME, FILE_NAME));
    }

    /**
     * Creates a configuration inside a specific directory, useful when the server root is not the
     * working directory.
     *
     * @param directory configuration directory
     * @return the configuration for {@code <directory>/tuff.yml}
     */
    public static TuffComputeConfig inDirectory(final Path directory) {
        return new TuffComputeConfig(directory.resolve(FILE_NAME));
    }

    /**
     * Returns the defaults of the shipped {@code tuff.yml}.
     *
     * <p>Identical to {@link ComputeSettings#defaults()} except for the shutdown grace period, which
     * is five seconds for a server because the world save runs right after the pool stops and
     * {@code /restart} only gives the shutdown about ten seconds in total. The plain record default
     * stays at thirty seconds for embedders that do not save a world on shutdown.
     *
     * @return the settings an untouched configuration file produces
     */
    public static ComputeSettings defaultSettings() {
        final ComputeSettings defaults = ComputeSettings.defaults();
        return new ComputeSettings(
            defaults.enabled(),
            defaults.workers(),
            defaults.queue(),
            defaults.scheduling(),
            defaults.consumers(),
            defaults.metrics(),
            defaults.adaptive(),
            SERVER_SHUTDOWN_GRACE
        );
    }

    /**
     * Returns the file this configuration reads and writes.
     *
     * @return absolute path of the configuration file
     */
    public Path file() {
        return this.file.toAbsolutePath();
    }

    /**
     * Reads the file, creates it from the bundled template when missing, publishes the result and
     * applies it.
     *
     * <p>This is the startup entry point. The first read has nothing to compare against, so its diff
     * is empty; calling it again after the file changed behaves like {@link #reload()}.
     *
     * @return what was read, changed and reported
     */
    public ConfigLoadResult load() {
        return this.readAndPublish(true);
    }

    /**
     * Re-reads the file and applies what the engine can adopt at runtime.
     *
     * <p>The diff is taken against the snapshot that is currently published, which is what a user
     * sees as "what changed since the last read". A file that cannot be parsed keeps the previous
     * snapshot.
     *
     * @return what was read, changed and reported
     */
    public ConfigLoadResult reload() {
        return this.readAndPublish(false);
    }

    /**
     * Returns the settings currently in effect.
     *
     * <p>The returned record is immutable and the reference is replaced as a whole on reload, so
     * callers on any thread see either the old or the new snapshot, never a mix.
     *
     * @return the current snapshot
     */
    public ComputeSettings settings() {
        return this.state.settings();
    }

    /**
     * Returns the region relief settings currently in effect.
     *
     * <p>Relief is carried outside {@link ComputeSettings} so the compute model stays usable without
     * it. The value follows the same reload rules: replaced as a whole, visible to any thread.
     *
     * @return the current relief settings, never {@code null}
     */
    public RegionReliefSettings reliefSettings() {
        return this.reliefSettings;
    }

    /**
     * Returns the overrun trigger policy currently in effect.
     *
     * <p>The trigger lives next to relief for the same reason: it is an optional subsystem and the
     * compute model must keep compiling without it. Like the relief settings, the value is replaced as
     * a whole on reload and is visible to any thread.
     *
     * @return the current trigger policy, never {@code null}
     */
    public RegionReliefSettings.Trigger triggerSettings() {
        return this.triggerSettings;
    }

    /**
     * Returns the problems of the last read.
     *
     * @return an immutable list, possibly empty
     */
    public List<ConfigProblem> problems() {
        return this.state.problems();
    }

    /**
     * Returns the diff produced by the last load or reload.
     *
     * @return the last diff, empty when nothing changed
     */
    public ConfigDiff lastDiff() {
        return this.state.diff();
    }

    /**
     * Sets the hook that applies changes to a running engine.
     *
     * <p>The hook is used by subsequent loads and reloads; setting it does not apply the current
     * snapshot. Pass {@code null} to detach it, for example after the pool has shut down.
     *
     * @param target apply hook or {@code null}
     */
    public void setApplyTarget(final @Nullable ComputeConfigApplyTarget target) {
        this.applyTarget = target;
    }

    /**
     * Resolves the worker count for automatic sizing against a CPU budget.
     *
     * <p>{@code threads: -1} in the file means "let the scheduler decide", and the scheduler decides
     * against this budget, not against the raw processor count. Build it with
     * {@link ComputeThreadBudget#resolve(int, int, int, int)}, which subtracts the region, chunk and
     * IO threads the server already runs. Configured {@code min-threads} and {@code max-threads}
     * still clamp the result.
     *
     * @param cpuBudget processors the compute pool may use, at least {@code 1}
     * @return the number of workers the pool should run with
     * @throws IllegalArgumentException if the budget is not positive or the configured bounds are
     *                                  inconsistent
     */
    public int resolveThreadCount(final int cpuBudget) {
        return this.settings().workers().resolveThreadCount(cpuBudget);
    }

    private ConfigLoadResult readAndPublish(final boolean initial) {
        synchronized (this.lock) {
            final ConfigLoadResult previous = this.state;
            final List<ConfigProblem> problems = new ArrayList<>();
            boolean created = false;
            ComputeSettings parsed = null;
            RegionReliefSettings parsedRelief = null;
            RegionReliefSettings.Trigger parsedTrigger = null;
            Configuration configuration = null;

            try {
                if (Files.notExists(this.file)) {
                    ConfigTemplate.writeDefault(this.file);
                    created = true;
                }
                configuration = ComputeConfigParser.load(this.file);
                parsed = ComputeConfigParser.parse(configuration, problems);
                parsedRelief = ComputeConfigParser.parseRelief(configuration, problems);
                parsedTrigger = ComputeConfigParser.parseTrigger(configuration, problems);
            } catch (final IOException ex) {
                problems.add(new ConfigProblem(this.file.toString(), "could not be read (" + ex + "), keeping the previous settings"));
            } catch (final InvalidConfigurationException ex) {
                problems.add(new ConfigProblem(this.file.toString(), "is not valid YAML (" + ex.getMessage() + "), keeping the previous settings"));
            } catch (final RuntimeException ex) {
                // our own bug or a hostile file, either way the server keeps the snapshot it has
                LOGGER.error("Unexpected failure while reading the compute configuration {}", this.file.toAbsolutePath(), ex);
                problems.add(new ConfigProblem(this.file.toString(), "could not be parsed (" + ex + "), keeping the previous settings"));
            }

            final boolean keptPrevious = parsed == null;
            final ComputeSettings settings = keptPrevious ? previous.settings() : parsed;
            final RegionReliefSettings relief = keptPrevious
                ? this.reliefSettings
                : (parsedRelief == null ? RegionReliefSettings.defaults() : parsedRelief);
            final RegionReliefSettings.Trigger trigger = keptPrevious
                ? this.triggerSettings
                : (parsedTrigger == null ? RegionReliefSettings.Trigger.defaults() : parsedTrigger);
            // the thread section lives outside ComputeSettings, so it is flattened from the same loaded
            // tree; a failed read must keep the old leaves too, otherwise a broken file would look like
            // "every thread key changed". relief settings are outside it for the same reason and are
            // merged into the same map so a reload sees them.
            final Map<String, String> flattening;
            if (keptPrevious) {
                flattening = this.flattened;
            } else {
                flattening = new LinkedHashMap<>(ComputeConfigParser.flatten(settings, configuration));
                flattening.putAll(ComputeConfigParser.flattenRelief(relief));
                flattening.putAll(ComputeConfigParser.flattenTrigger(trigger));
            }
            final boolean firstRead = !this.loaded;
            final ConfigDiff diff = firstRead
                ? ConfigDiff.empty()
                : ConfigDiff.between(this.flattened, flattening);
            final ConfigLoadResult result = new ConfigLoadResult(settings, problems, diff, created, keptPrevious);

            this.loaded = true;
            this.flattened = Map.copyOf(flattening);
            this.reliefSettings = relief;
            this.triggerSettings = trigger;
            this.state = result;
            this.log(result, initial);
            this.apply(previous.settings(), result);
            return result;
        }
    }

    private void apply(final ComputeSettings previous, final ConfigLoadResult result) {
        final ComputeConfigApplyTarget target = this.applyTarget;
        if (target == null || result.keptPreviousSettings() || result.diff().isEmpty()) {
            return;
        }
        try {
            target.apply(previous, result.settings(), result.diff());
        } catch (final RuntimeException ex) {
            // the new snapshot stays published on purpose: the next reload or a restart brings the
            // engine back in line, and failing the whole switch would be worse for the server
            LOGGER.error("Failed to apply the compute configuration to the scheduler", ex);
        }
    }

    private void log(final ConfigLoadResult result, final boolean initial) {
        if (result.createdFile()) {
            LOGGER.info("Created the default compute configuration at {}", this.file.toAbsolutePath());
        }
        for (final ConfigProblem problem : result.problems()) {
            LOGGER.warn("Compute config: {}", problem);
        }
        if (result.keptPreviousSettings()) {
            LOGGER.warn("Compute configuration was not reloaded, the scheduler keeps its previous settings");
            return;
        }
        if (initial) {
            LOGGER.info("Compute configuration loaded from {}", this.file.toAbsolutePath());
            return;
        }
        if (result.diff().isEmpty()) {
            LOGGER.info("Compute configuration reloaded, nothing changed");
            return;
        }
        LOGGER.info("Compute configuration reloaded, {} change(s): {}", result.diff().changedPaths().size(), result.diff().summary());
        if (!result.diff().restartRequiredPaths().isEmpty()) {
            LOGGER.warn("These compute settings take effect after a restart: {}", result.diff().restartSummary());
        }
    }
}
