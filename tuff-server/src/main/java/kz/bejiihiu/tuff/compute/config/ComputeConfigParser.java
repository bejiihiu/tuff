package kz.bejiihiu.tuff.compute.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import org.bukkit.configuration.Configuration;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.jspecify.annotations.Nullable;

/**
 * Translates {@code config/tuff.yml} into {@link ComputeSettings}.
 *
 * <p>The reader is deliberately separate from the file handling: this class only sees a loaded
 * {@link Configuration} and never writes anything. Values are read one key at a time, so a wrong
 * type costs that key and not the whole file, and every section record still validates itself. A
 * section that is internally inconsistent (say {@code threads} above {@code max-threads}) is
 * replaced by its defaults and reported, exactly like a single unreadable key.
 *
 * <p>Keys are mapped one to one onto the record tree, with two documented translations: enum values
 * are written in kebab case ({@code weighted-fair} for {@code WEIGHTED_FAIR}), and
 * {@code consumers.<id>.max-concurrent} is a fixed count when written as a whole number or a share
 * of the pool when written with a decimal point.
 */
final class ComputeConfigParser {

    static final String SECTION = "async-compute";
    private static final String THREAD_MANAGEMENT = "thread-management";

    private static final String WORKERS_PATH = SECTION + ".workers";
    private static final String QUEUE_PATH = SECTION + ".queue";
    private static final String SCHEDULING_PATH = SECTION + ".scheduling";
    private static final String CONSUMERS_PATH = SECTION + ".consumers";
    private static final String METRICS_PATH = SECTION + ".metrics";
    private static final String ADAPTIVE_PATH = SECTION + ".adaptive";
    private static final String LANES_PATH = SECTION + ".lanes";
    private static final String LANE_VIRTUAL_PATH = LANES_PATH + ".virtual";
    private static final String RELIEF_PATH = SECTION + ".relief";
    private static final String RELIEF_CONSUMER_PATH = RELIEF_PATH + ".consumer";
    private static final String TRIGGER_PATH = RELIEF_PATH + ".trigger";

    private static final String DEFAULT_CONSUMER = "default";
    private static final String ALLOW_DYNAMIC_REGISTRATION = "allow-dynamic-registration";

    private static final Set<String> ROOT_KEYS = Set.of(SECTION, THREAD_MANAGEMENT);
    private static final Set<String> SETTINGS_KEYS = Set.of(
        "enabled", "workers", "queue", "scheduling", "consumers", "metrics", "adaptive", "lanes", "relief",
        "shutdown-grace-seconds"
    );
    private static final Set<String> WORKER_KEYS = Set.of(
        "threads", "min-threads", "max-threads", "keep-alive-seconds", "thread-name-prefix"
    );
    private static final Set<String> QUEUE_KEYS = Set.of("capacity", "overflow-policy");
    private static final Set<String> SCHEDULING_KEYS = Set.of(
        "strategy", "work-stealing", "priority-aging", "aging-interval-millis", "max-aging-boost", "idle-wait-millis"
    );
    private static final Set<String> CONSUMERS_KEYS = Set.of(ALLOW_DYNAMIC_REGISTRATION, DEFAULT_CONSUMER);
    private static final Set<String> CONSUMER_KEYS = Set.of(
        "enabled", "max-concurrent", "fair-share-weight", "max-queued-tasks", "overflow-policy", "max-queue-wait-seconds"
    );
    private static final Set<String> METRICS_KEYS = Set.of(
        "enabled", "jfr", "throughput-window-seconds", "sample-cpu", "sample-interval-seconds"
    );
    private static final Set<String> ADAPTIVE_KEYS = Set.of(
        "enabled", "min-concurrency", "max-concurrency", "sample-window-seconds", "min-samples",
        "increase-step", "decrease-factor", "target-utilization"
    );
    private static final Set<String> LANES_KEYS = Set.of("virtual");
    private static final Set<String> LANE_VIRTUAL_KEYS = Set.of(
        "enabled", "low-priority", "normal-spill", "max-concurrent", "spill-wait-millis", "spill-queue-depth"
    );
    private static final Set<String> RELIEF_KEYS = Set.of(
        "enabled", "max-in-flight-per-region", "reserved-cores", "consumer", "trigger"
    );
    private static final Set<String> TRIGGER_KEYS = Set.of(
        "enabled", "overrun-millis", "enter-overruns", "exit-calm-ticks", "cooldown-seconds",
        "max-windows", "rate-window-seconds", "stale-after-seconds"
    );

    private ComputeConfigParser() {
    }

    /**
     * Reads the file as UTF-8 and returns the loaded tree.
     *
     * <p>Split from {@link #parse(Configuration, List)} so one read can serve both the settings and
     * the raw {@code thread-management} section the diff needs; the section is owned by another parser
     * and is not part of {@link ComputeSettings}.
     *
     * @param file file to read
     * @return the loaded configuration, never {@code null}
     * @throws IOException                   if the file cannot be read
     * @throws InvalidConfigurationException if the file is not valid YAML
     */
    static YamlConfiguration load(final Path file) throws IOException, InvalidConfigurationException {
        final YamlConfiguration configuration = new YamlConfiguration();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            configuration.load(reader);
        }
        return configuration;
    }

    /**
     * Parses an already loaded configuration tree.
     *
     * @param configuration loaded YAML
     * @param problems      problem sink, appended to in place
     * @return the parsed settings, never {@code null}
     */
    static ComputeSettings parse(final Configuration configuration, final List<ConfigProblem> problems) {
        reportUnknownKeys(configuration, problems);

        final ComputeSettings defaults = TuffComputeConfig.defaultSettings();
        final ConfigurationSection root = readSection(configuration, SECTION, SECTION, problems);
        if (root == null) {
            problems.add(new ConfigProblem(SECTION, "section is missing, using defaults"));
            return defaults;
        }

        final boolean enabled = readBoolean(root, "enabled", defaults.enabled(), SECTION + ".enabled", problems);
        final ComputeSettings.Workers workers = parseWorkers(readSection(root, "workers", WORKERS_PATH, problems), defaults.workers(), problems);
        final ComputeSettings.QueueSettings queue = parseQueue(readSection(root, "queue", QUEUE_PATH, problems), defaults.queue(), problems);
        final ComputeSettings.Scheduling scheduling = parseScheduling(readSection(root, "scheduling", SCHEDULING_PATH, problems), defaults.scheduling(), problems);
        final ComputeSettings.Consumers consumers = parseConsumers(readSection(root, "consumers", CONSUMERS_PATH, problems), defaults.consumers(), problems);
        final ComputeSettings.Metrics metrics = parseMetrics(readSection(root, "metrics", METRICS_PATH, problems), defaults.metrics(), problems);
        final ComputeSettings.Adaptive adaptive = parseAdaptive(readSection(root, "adaptive", ADAPTIVE_PATH, problems), defaults.adaptive(), problems);
        final ComputeSettings.Lanes lanes = parseLanes(readSection(root, "lanes", LANES_PATH, problems), defaults.lanes(), problems);
        final Duration grace = readSeconds(root, "shutdown-grace-seconds", defaults.shutdownGracePeriod(), true, SECTION + ".shutdown-grace-seconds", problems);

        try {
            return new ComputeSettings(enabled, workers, queue, scheduling, consumers, metrics, adaptive, lanes, grace);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            // sections are validated on their own, so this only catches a cross-section mistake we do
            // not know about - keep the server alive and say so
            problems.add(new ConfigProblem(SECTION, "settings rejected (" + ex.getMessage() + "), using defaults"));
            return defaults;
        }
    }

    /**
     * Renders settings as dotted keys, used for diffs and for the default file fallback.
     *
     * @param settings settings to render
     * @return ordered map of dotted key to display value
     */
    static Map<String, String> flatten(final ComputeSettings settings) {
        return flatten(settings, null);
    }

    /**
     * Renders settings as dotted keys and appends the raw {@code thread-management} section.
     *
     * <p>{@link ComputeSettings} does not own the thread section - the thread layer parses it itself -
     * but the diff still has to see it: the whole section is restart only, and without its keys
     * {@code /tuff reload} would report "nothing changed" after an administrator edited it. The leaves
     * are read from the same loaded tree, so no second file read and no second parser are involved.
     * Values are compared as text; the section has no live reload, so "which exactly" does not matter,
     * only "any of them".
     *
     * @param settings      settings to render
     * @param configuration loaded file the thread section is read from, or {@code null} to skip it
     * @return ordered map of dotted key to display value
     */
    static Map<String, String> flatten(final ComputeSettings settings, final @Nullable Configuration configuration) {
        final Map<String, String> flat = new LinkedHashMap<>();
        flat.put(SECTION + ".enabled", Boolean.toString(settings.enabled()));

        final ComputeSettings.Workers workers = settings.workers();
        flat.put(WORKERS_PATH + ".threads", Integer.toString(workers.threads()));
        flat.put(WORKERS_PATH + ".min-threads", Integer.toString(workers.minThreads()));
        flat.put(WORKERS_PATH + ".max-threads", Integer.toString(workers.maxThreads()));
        flat.put(WORKERS_PATH + ".keep-alive-seconds", formatSeconds(workers.keepAlive()));
        flat.put(WORKERS_PATH + ".thread-name-prefix", workers.threadNamePrefix());

        flat.put(QUEUE_PATH + ".capacity", Integer.toString(settings.queue().capacity()));
        flat.put(QUEUE_PATH + ".overflow-policy", formatOverflow(settings.queue().overflowPolicy()));

        final ComputeSettings.Scheduling scheduling = settings.scheduling();
        flat.put(SCHEDULING_PATH + ".strategy", formatStrategy(scheduling.strategy()));
        flat.put(SCHEDULING_PATH + ".work-stealing", Boolean.toString(scheduling.workStealing()));
        flat.put(SCHEDULING_PATH + ".priority-aging", Boolean.toString(scheduling.priorityAging()));
        flat.put(SCHEDULING_PATH + ".aging-interval-millis", Long.toString(scheduling.agingInterval().toMillis()));
        flat.put(SCHEDULING_PATH + ".max-aging-boost", Integer.toString(scheduling.maxAgingBoost()));
        flat.put(SCHEDULING_PATH + ".idle-wait-millis", Long.toString(scheduling.idleWait().toMillis()));

        final ComputeSettings.Consumers consumers = settings.consumers();
        flat.put(CONSUMERS_PATH + "." + ALLOW_DYNAMIC_REGISTRATION, Boolean.toString(consumers.allowDynamicRegistration()));
        flattenPolicy(flat, CONSUMERS_PATH + "." + DEFAULT_CONSUMER, consumers.defaultPolicy());
        for (final Map.Entry<String, ConsumerPolicy> override : new TreeMap<>(consumers.overrides()).entrySet()) {
            flattenPolicy(flat, CONSUMERS_PATH + "." + override.getKey(), override.getValue());
        }

        final ComputeSettings.Metrics metrics = settings.metrics();
        flat.put(METRICS_PATH + ".enabled", Boolean.toString(metrics.enabled()));
        flat.put(METRICS_PATH + ".jfr", Boolean.toString(metrics.jfrEnabled()));
        flat.put(METRICS_PATH + ".throughput-window-seconds", formatSeconds(metrics.throughputWindow()));
        flat.put(METRICS_PATH + ".sample-cpu", Boolean.toString(metrics.sampleCpuUtilization()));
        flat.put(METRICS_PATH + ".sample-interval-seconds", formatSeconds(metrics.sampleInterval()));

        final ComputeSettings.Adaptive adaptive = settings.adaptive();
        flat.put(ADAPTIVE_PATH + ".enabled", Boolean.toString(adaptive.enabled()));
        flat.put(ADAPTIVE_PATH + ".min-concurrency", Integer.toString(adaptive.minConcurrency()));
        flat.put(ADAPTIVE_PATH + ".max-concurrency", Integer.toString(adaptive.maxConcurrency()));
        flat.put(ADAPTIVE_PATH + ".sample-window-seconds", formatSeconds(adaptive.sampleWindow()));
        flat.put(ADAPTIVE_PATH + ".min-samples", Integer.toString(adaptive.minSamples()));
        flat.put(ADAPTIVE_PATH + ".increase-step", Double.toString(adaptive.increaseStep()));
        flat.put(ADAPTIVE_PATH + ".decrease-factor", Double.toString(adaptive.decreaseFactor()));
        flat.put(ADAPTIVE_PATH + ".target-utilization", Double.toString(adaptive.targetUtilization()));

        final ComputeSettings.Lanes.Virtual virtual = settings.lanes().virtual();
        flat.put(LANE_VIRTUAL_PATH + ".enabled", Boolean.toString(virtual.enabled()));
        flat.put(LANE_VIRTUAL_PATH + ".low-priority", Boolean.toString(virtual.lowPriority()));
        flat.put(LANE_VIRTUAL_PATH + ".normal-spill", Boolean.toString(virtual.normalSpill()));
        flat.put(LANE_VIRTUAL_PATH + ".max-concurrent", Integer.toString(virtual.maxConcurrent()));
        flat.put(LANE_VIRTUAL_PATH + ".spill-wait-millis", Long.toString(virtual.spillWait().toMillis()));
        flat.put(LANE_VIRTUAL_PATH + ".spill-queue-depth", Integer.toString(virtual.spillQueueDepth()));

        flat.put(SECTION + ".shutdown-grace-seconds", formatSeconds(settings.shutdownGracePeriod()));
        flattenThreadSection(flat, configuration);
        return flat;
    }

    /**
     * Adds every leaf of {@code thread-management} from the loaded tree.
     *
     * <p>Walking the tree instead of a fixed key list is on purpose: a key the thread layer starts
     * honouring tomorrow must not silently fall outside the diff. The section is restart only as a
     * whole, so an unrecognised leaf still only ever produces a restart notice.
     *
     * @param flat          map to append to
     * @param configuration loaded file, or {@code null}
     */
    private static void flattenThreadSection(final Map<String, String> flat, final @Nullable Configuration configuration) {
        if (configuration == null) {
            return;
        }
        final ConfigurationSection section = configuration.getConfigurationSection(THREAD_MANAGEMENT);
        if (section == null) {
            return;
        }
        for (final String key : section.getKeys(true)) {
            if (section.isConfigurationSection(key)) {
                continue;
            }
            flat.put(THREAD_MANAGEMENT + "." + key, String.valueOf(section.get(key)));
        }
    }

    private static void flattenPolicy(final Map<String, String> flat, final String path, final ConsumerPolicy policy) {
        flat.put(path + ".enabled", Boolean.toString(policy.enabled()));
        flat.put(path + ".max-concurrent", formatLimit(policy.concurrencyLimit()));
        flat.put(path + ".fair-share-weight", Double.toString(policy.fairShareWeight()));
        flat.put(path + ".max-queued-tasks", Integer.toString(policy.maxQueuedTasks()));
        flat.put(path + ".overflow-policy", formatOverflow(policy.overflowPolicy()));
        flat.put(path + ".max-queue-wait-seconds", formatSeconds(policy.maxQueueWait()));
    }

    private static String formatLimit(final ConsumerPolicy.ConcurrencyLimit limit) {
        return switch (limit) {
            case final ConsumerPolicy.AbsoluteLimit absolute -> Integer.toString(absolute.permits());
            case final ConsumerPolicy.RelativeLimit relative -> Double.toString(relative.fraction());
        };
    }

    private static String formatStrategy(final ComputeSettings.Scheduling.Strategy strategy) {
        return strategy.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    private static String formatOverflow(final OverflowPolicy policy) {
        return policy.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    private static String formatSeconds(final Duration duration) {
        final long millis = duration.toMillis();
        if (millis % 1000L == 0L) {
            return Long.toString(millis / 1000L);
        }
        return millis + "ms";
    }

    private static ComputeSettings.Workers parseWorkers(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Workers fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final int threads = readInt(section, "threads", fallback.threads(), WORKERS_PATH + ".threads", problems);
        final int minThreads = readInt(section, "min-threads", fallback.minThreads(), WORKERS_PATH + ".min-threads", problems);
        final int maxThreads = readInt(section, "max-threads", fallback.maxThreads(), WORKERS_PATH + ".max-threads", problems);
        final Duration keepAlive = readSeconds(section, "keep-alive-seconds", fallback.keepAlive(), false, WORKERS_PATH + ".keep-alive-seconds", problems);
        final String prefix = readString(section, "thread-name-prefix", fallback.threadNamePrefix(), WORKERS_PATH + ".thread-name-prefix", problems);
        try {
            return new ComputeSettings.Workers(threads, minThreads, maxThreads, keepAlive, prefix);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(WORKERS_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ComputeSettings.QueueSettings parseQueue(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.QueueSettings fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final int capacity = readInt(section, "capacity", fallback.capacity(), QUEUE_PATH + ".capacity", problems);
        final OverflowPolicy overflow = readOverflow(section, "overflow-policy", fallback.overflowPolicy(), QUEUE_PATH + ".overflow-policy", problems);
        try {
            return new ComputeSettings.QueueSettings(capacity, overflow);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(QUEUE_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ComputeSettings.Scheduling parseScheduling(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Scheduling fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final ComputeSettings.Scheduling.Strategy strategy = readStrategy(section, fallback.strategy(), problems);
        final boolean workStealing = readBoolean(section, "work-stealing", fallback.workStealing(), SCHEDULING_PATH + ".work-stealing", problems);
        final boolean aging = readBoolean(section, "priority-aging", fallback.priorityAging(), SCHEDULING_PATH + ".priority-aging", problems);
        final Duration agingInterval = readMillis(section, "aging-interval-millis", fallback.agingInterval(), false, SCHEDULING_PATH + ".aging-interval-millis", problems);
        final int maxAgingBoost = readInt(section, "max-aging-boost", fallback.maxAgingBoost(), SCHEDULING_PATH + ".max-aging-boost", problems);
        final Duration idleWait = readMillis(section, "idle-wait-millis", fallback.idleWait(), false, SCHEDULING_PATH + ".idle-wait-millis", problems);
        try {
            return new ComputeSettings.Scheduling(strategy, workStealing, aging, agingInterval, maxAgingBoost, idleWait);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(SCHEDULING_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ComputeSettings.Metrics parseMetrics(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Metrics fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), METRICS_PATH + ".enabled", problems);
        final boolean jfr = readBoolean(section, "jfr", fallback.jfrEnabled(), METRICS_PATH + ".jfr", problems);
        final Duration window = readSeconds(section, "throughput-window-seconds", fallback.throughputWindow(), false, METRICS_PATH + ".throughput-window-seconds", problems);
        final boolean sampleCpu = readBoolean(section, "sample-cpu", fallback.sampleCpuUtilization(), METRICS_PATH + ".sample-cpu", problems);
        final Duration interval = readSeconds(section, "sample-interval-seconds", fallback.sampleInterval(), false, METRICS_PATH + ".sample-interval-seconds", problems);
        try {
            return new ComputeSettings.Metrics(enabled, jfr, window, sampleCpu, interval);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(METRICS_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ComputeSettings.Adaptive parseAdaptive(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Adaptive fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), ADAPTIVE_PATH + ".enabled", problems);
        final int minConcurrency = readInt(section, "min-concurrency", fallback.minConcurrency(), ADAPTIVE_PATH + ".min-concurrency", problems);
        int maxConcurrency = readInt(section, "max-concurrency", fallback.maxConcurrency(), ADAPTIVE_PATH + ".max-concurrency", problems);
        if (maxConcurrency != -1 && maxConcurrency < minConcurrency) {
            // -1 is "the whole pool, no personal cap"; anything else must leave room for the floor,
            // otherwise the record would throw and take every other adaptive key down with it
            problems.add(new ConfigProblem(
                ADAPTIVE_PATH + ".max-concurrency",
                "must be -1 (the whole worker pool, no personal cap) or at least min-concurrency ("
                    + minConcurrency + "), got " + maxConcurrency + "; using " + describeConcurrency(fallback.maxConcurrency())
            ));
            maxConcurrency = fallback.maxConcurrency();
        }
        final Duration window = readSeconds(section, "sample-window-seconds", fallback.sampleWindow(), false, ADAPTIVE_PATH + ".sample-window-seconds", problems);
        final int minSamples = readInt(section, "min-samples", fallback.minSamples(), ADAPTIVE_PATH + ".min-samples", problems);
        final double increase = readDouble(section, "increase-step", fallback.increaseStep(), ADAPTIVE_PATH + ".increase-step", problems);
        final double decrease = readDouble(section, "decrease-factor", fallback.decreaseFactor(), ADAPTIVE_PATH + ".decrease-factor", problems);
        final double target = readDouble(section, "target-utilization", fallback.targetUtilization(), ADAPTIVE_PATH + ".target-utilization", problems);
        try {
            return new ComputeSettings.Adaptive(enabled, minConcurrency, maxConcurrency, window, minSamples, increase, decrease, target);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(ADAPTIVE_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ComputeSettings.Lanes parseLanes(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Lanes fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final ComputeSettings.Lanes.Virtual virtual = parseVirtualLane(
            readSection(section, "virtual", LANE_VIRTUAL_PATH, problems),
            fallback.virtual(),
            problems
        );
        try {
            return new ComputeSettings.Lanes(virtual);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(LANES_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    /**
     * Parses the {@code async-compute.relief} section.
     *
     * <p>Split from {@link #parse(Configuration, List)} because relief settings are not part of
     * {@link ComputeSettings}: the compute model stays free of an optional subsystem, and the parser
     * still reports every relief key in the same problem list.
     *
     * @param configuration loaded YAML
     * @param problems      problem sink, appended to in place
     * @return the parsed relief settings, never {@code null}
     */
    static RegionReliefSettings parseRelief(final Configuration configuration, final List<ConfigProblem> problems) {
        final RegionReliefSettings fallback = RegionReliefSettings.defaults();
        final ConfigurationSection root = configuration.getConfigurationSection(SECTION);
        if (root == null) {
            return fallback;
        }
        final ConfigurationSection section = root.getConfigurationSection("relief");
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), RELIEF_PATH + ".enabled", problems);
        int maxInFlight = readInt(
            section, "max-in-flight-per-region", fallback.maxInFlightPerRegion(),
            RELIEF_PATH + ".max-in-flight-per-region", problems
        );
        int reserved = readInt(
            section, "reserved-cores", fallback.reservedCores(), RELIEF_PATH + ".reserved-cores", problems
        );
        final ConsumerPolicy policy = parsePolicy(
            readSection(section, "consumer", RELIEF_CONSUMER_PATH, problems),
            fallback.consumerPolicy(),
            RELIEF_CONSUMER_PATH,
            problems
        );
        if (maxInFlight < 1) {
            problems.add(new ConfigProblem(
                RELIEF_PATH + ".max-in-flight-per-region",
                "must be at least 1, got " + maxInFlight + "; using " + fallback.maxInFlightPerRegion()
            ));
            maxInFlight = fallback.maxInFlightPerRegion();
        }
        if (reserved < 0) {
            problems.add(new ConfigProblem(
                RELIEF_PATH + ".reserved-cores",
                "must not be negative, got " + reserved + "; using " + fallback.reservedCores()
            ));
            reserved = fallback.reservedCores();
        }
        try {
            return new RegionReliefSettings(enabled, maxInFlight, reserved, policy);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(RELIEF_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    /**
     * Renders the relief settings as dotted keys, so a reload can see them change.
     *
     * @param relief relief settings to render
     * @return ordered map of dotted key to display value
     * @throws NullPointerException if {@code relief} is {@code null}
     */
    static Map<String, String> flattenRelief(final RegionReliefSettings relief) {
        Objects.requireNonNull(relief, "relief");
        final Map<String, String> flat = new LinkedHashMap<>();
        flat.put(RELIEF_PATH + ".enabled", Boolean.toString(relief.enabled()));
        flat.put(RELIEF_PATH + ".max-in-flight-per-region", Integer.toString(relief.maxInFlightPerRegion()));
        flat.put(RELIEF_PATH + ".reserved-cores", Integer.toString(relief.reservedCores()));
        flattenPolicy(flat, RELIEF_CONSUMER_PATH, relief.consumerPolicy());
        return flat;
    }

    /**
     * Parses the {@code async-compute.relief.trigger} section.
     *
     * <p>The trigger decides when a region is behind; it is not part of the relief service, so it is
     * parsed on its own. Every key falls back to its own default and is reported, and a combination the
     * record rejects falls back to the whole default policy with one problem entry.
     *
     * @param configuration loaded YAML
     * @param problems      problem sink, appended to in place
     * @return the parsed trigger policy, never {@code null}
     */
    static RegionReliefSettings.Trigger parseTrigger(
        final Configuration configuration,
        final List<ConfigProblem> problems
    ) {
        final RegionReliefSettings.Trigger fallback = RegionReliefSettings.Trigger.defaults();
        final ConfigurationSection root = configuration.getConfigurationSection(SECTION);
        if (root == null) {
            return fallback;
        }
        final ConfigurationSection relief = root.getConfigurationSection("relief");
        if (relief == null) {
            return fallback;
        }
        final ConfigurationSection section = readSection(relief, "trigger", TRIGGER_PATH, problems);
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), TRIGGER_PATH + ".enabled", problems);
        final Duration overrun = readMillis(
            section, "overrun-millis", fallback.overrun(), false, TRIGGER_PATH + ".overrun-millis", problems
        );
        int enter = readInt(
            section, "enter-overruns", fallback.enterOverruns(), TRIGGER_PATH + ".enter-overruns", problems
        );
        int exit = readInt(
            section, "exit-calm-ticks", fallback.exitCalm(), TRIGGER_PATH + ".exit-calm-ticks", problems
        );
        final Duration cooldown = readSeconds(
            section, "cooldown-seconds", fallback.cooldown(), true, TRIGGER_PATH + ".cooldown-seconds", problems
        );
        int maxWindows = readInt(
            section, "max-windows", fallback.maxWindows(), TRIGGER_PATH + ".max-windows", problems
        );
        final Duration rateWindow = readSeconds(
            section, "rate-window-seconds", fallback.rateWindow(), false, TRIGGER_PATH + ".rate-window-seconds", problems
        );
        final Duration staleAfter = readSeconds(
            section, "stale-after-seconds", fallback.staleAfter(), false, TRIGGER_PATH + ".stale-after-seconds", problems
        );
        enter = atLeastOne(enter, fallback.enterOverruns(), TRIGGER_PATH + ".enter-overruns", problems);
        exit = atLeastOne(exit, fallback.exitCalm(), TRIGGER_PATH + ".exit-calm-ticks", problems);
        maxWindows = atLeastOne(maxWindows, fallback.maxWindows(), TRIGGER_PATH + ".max-windows", problems);
        try {
            return new RegionReliefSettings.Trigger(
                enabled, overrun, enter, exit, cooldown, maxWindows, rateWindow, staleAfter
            );
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(TRIGGER_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    /**
     * Renders the trigger policy as dotted keys, so a reload can see them change.
     *
     * @param trigger trigger policy to render
     * @return ordered map of dotted key to display value
     * @throws NullPointerException if {@code trigger} is {@code null}
     */
    static Map<String, String> flattenTrigger(final RegionReliefSettings.Trigger trigger) {
        Objects.requireNonNull(trigger, "trigger");
        final Map<String, String> flat = new LinkedHashMap<>();
        flat.put(TRIGGER_PATH + ".enabled", Boolean.toString(trigger.enabled()));
        flat.put(TRIGGER_PATH + ".overrun-millis", Long.toString(trigger.overrun().toMillis()));
        flat.put(TRIGGER_PATH + ".enter-overruns", Integer.toString(trigger.enterOverruns()));
        flat.put(TRIGGER_PATH + ".exit-calm-ticks", Integer.toString(trigger.exitCalm()));
        flat.put(TRIGGER_PATH + ".cooldown-seconds", formatSeconds(trigger.cooldown()));
        flat.put(TRIGGER_PATH + ".max-windows", Integer.toString(trigger.maxWindows()));
        flat.put(TRIGGER_PATH + ".rate-window-seconds", formatSeconds(trigger.rateWindow()));
        flat.put(TRIGGER_PATH + ".stale-after-seconds", formatSeconds(trigger.staleAfter()));
        return flat;
    }

    private static int atLeastOne(final int value, final int fallback, final String path, final List<ConfigProblem> problems) {
        if (value >= 1) {
            return value;
        }
        problems.add(new ConfigProblem(path, "must be at least 1, got " + value + "; using " + fallback));
        return fallback;
    }

    private static ComputeSettings.Lanes.Virtual parseVirtualLane(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Lanes.Virtual fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), LANE_VIRTUAL_PATH + ".enabled", problems);
        final boolean lowPriority = readBoolean(section, "low-priority", fallback.lowPriority(), LANE_VIRTUAL_PATH + ".low-priority", problems);
        final boolean normalSpill = readBoolean(section, "normal-spill", fallback.normalSpill(), LANE_VIRTUAL_PATH + ".normal-spill", problems);
        int maxConcurrent = readInt(section, "max-concurrent", fallback.maxConcurrent(), LANE_VIRTUAL_PATH + ".max-concurrent", problems);
        final Duration spillWait = readMillis(section, "spill-wait-millis", fallback.spillWait(), true, LANE_VIRTUAL_PATH + ".spill-wait-millis", problems);
        int spillQueueDepth = readInt(section, "spill-queue-depth", fallback.spillQueueDepth(), LANE_VIRTUAL_PATH + ".spill-queue-depth", problems);
        // clamp the two counts at their own keys; losing the whole lane over one bad number would
        // also reset the thresholds that were written correctly
        if (maxConcurrent < 1) {
            problems.add(new ConfigProblem(
                LANE_VIRTUAL_PATH + ".max-concurrent",
                "must be at least 1, got " + maxConcurrent + "; using " + fallback.maxConcurrent()
            ));
            maxConcurrent = fallback.maxConcurrent();
        }
        if (spillQueueDepth < 0) {
            problems.add(new ConfigProblem(
                LANE_VIRTUAL_PATH + ".spill-queue-depth",
                "must not be negative, got " + spillQueueDepth + "; using " + fallback.spillQueueDepth()
            ));
            spillQueueDepth = fallback.spillQueueDepth();
        }
        try {
            return new ComputeSettings.Lanes.Virtual(enabled, lowPriority, normalSpill, maxConcurrent, spillWait, spillQueueDepth);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(LANE_VIRTUAL_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ComputeSettings.Consumers parseConsumers(
        final @Nullable ConfigurationSection section,
        final ComputeSettings.Consumers fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean allowDynamic = readBoolean(
            section, ALLOW_DYNAMIC_REGISTRATION, fallback.allowDynamicRegistration(), CONSUMERS_PATH + "." + ALLOW_DYNAMIC_REGISTRATION, problems
        );
        final ConsumerPolicy defaults = parsePolicy(
            readSection(section, DEFAULT_CONSUMER, CONSUMERS_PATH + "." + DEFAULT_CONSUMER, problems),
            fallback.defaultPolicy(),
            CONSUMERS_PATH + "." + DEFAULT_CONSUMER,
            problems
        );

        final Map<String, ConsumerPolicy> overrides = new LinkedHashMap<>();
        for (final String consumerId : section.getKeys(false)) {
            if (ALLOW_DYNAMIC_REGISTRATION.equals(consumerId) || DEFAULT_CONSUMER.equals(consumerId)) {
                continue;
            }
            final ConfigurationSection consumer = readSection(section, consumerId, CONSUMERS_PATH + "." + consumerId, problems);
            if (consumer == null) {
                continue;
            }
            overrides.put(consumerId, parsePolicy(consumer, defaults, CONSUMERS_PATH + "." + consumerId, problems));
        }
        try {
            return new ComputeSettings.Consumers(defaults, overrides, allowDynamic);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(CONSUMERS_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ConsumerPolicy parsePolicy(
        final @Nullable ConfigurationSection section,
        final ConsumerPolicy fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), path + ".enabled", problems);
        final ConsumerPolicy.ConcurrencyLimit limit = readLimit(section, "max-concurrent", fallback.concurrencyLimit(), path + ".max-concurrent", problems);
        final double weight = readDouble(section, "fair-share-weight", fallback.fairShareWeight(), path + ".fair-share-weight", problems);
        final int maxQueued = readInt(section, "max-queued-tasks", fallback.maxQueuedTasks(), path + ".max-queued-tasks", problems);
        final OverflowPolicy overflow = readOverflow(section, "overflow-policy", fallback.overflowPolicy(), path + ".overflow-policy", problems);
        final Duration maxWait = readSeconds(section, "max-queue-wait-seconds", fallback.maxQueueWait(), true, path + ".max-queue-wait-seconds", problems);
        try {
            return new ConsumerPolicy(enabled, limit, maxQueued, weight, overflow, maxWait);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(path, "rejected (" + ex.getMessage() + "), using the inherited policy"));
            return fallback;
        }
    }

    private static ConsumerPolicy.ConcurrencyLimit readLimit(
        final ConfigurationSection section,
        final String key,
        final ConsumerPolicy.ConcurrencyLimit fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof Double || raw instanceof Float) {
            return relativeLimit(((Number) raw).doubleValue(), path, fallback, problems);
        }
        if (raw instanceof Number number) {
            return absoluteLimit(number.longValue(), path, fallback, problems);
        }
        if (raw instanceof String text) {
            final String trimmed = text.trim();
            try {
                if (trimmed.indexOf('.') >= 0 || trimmed.indexOf('e') >= 0 || trimmed.indexOf('E') >= 0) {
                    return relativeLimit(Double.parseDouble(trimmed), path, fallback, problems);
                }
                return absoluteLimit(Long.parseLong(trimmed), path, fallback, problems);
            } catch (NumberFormatException _) {
                problems.add(new ConfigProblem(path, "expected a whole number or a fraction, got '" + text + "'; using " + describe(fallback)));
                return fallback;
            }
        }
        problems.add(new ConfigProblem(path, "expected a whole number or a fraction, got '" + raw + "'; using " + describe(fallback)));
        return fallback;
    }

    private static ConsumerPolicy.ConcurrencyLimit relativeLimit(
        final double fraction,
        final String path,
        final ConsumerPolicy.ConcurrencyLimit fallback,
        final List<ConfigProblem> problems
    ) {
        if (!Double.isFinite(fraction) || fraction <= 0.0d || fraction > 1.0d) {
            problems.add(new ConfigProblem(path, "a share must be in (0.0, 1.0], got " + fraction + "; using " + describe(fallback)));
            return fallback;
        }
        return ConsumerPolicy.ConcurrencyLimit.relative(fraction);
    }

    private static ConsumerPolicy.ConcurrencyLimit absoluteLimit(
        final long permits,
        final String path,
        final ConsumerPolicy.ConcurrencyLimit fallback,
        final List<ConfigProblem> problems
    ) {
        if (permits == -1L) {
            return ConsumerPolicy.ConcurrencyLimit.automatic();
        }
        if (permits < 1L || permits > Integer.MAX_VALUE) {
            problems.add(new ConfigProblem(path, "must be -1, a share in (0.0, 1.0] or at least 1, got " + permits + "; using " + describe(fallback)));
            return fallback;
        }
        return ConsumerPolicy.ConcurrencyLimit.absolute((int) permits);
    }

    private static String describe(final ConsumerPolicy.ConcurrencyLimit limit) {
        return switch (limit) {
            case final ConsumerPolicy.AbsoluteLimit absolute -> Integer.toString(absolute.permits());
            case final ConsumerPolicy.RelativeLimit relative -> Double.toString(relative.fraction());
        };
    }

    private static String describeConcurrency(final int maxConcurrency) {
        return maxConcurrency == -1 ? "-1 (the whole pool)" : Integer.toString(maxConcurrency);
    }

    private static ComputeSettings.Scheduling.Strategy readStrategy(
        final ConfigurationSection section,
        final ComputeSettings.Scheduling.Strategy fallback,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get("strategy");
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof String text) {
            final String normalized = text.trim().toUpperCase(Locale.ROOT).replace('-', '_');
            for (final ComputeSettings.Scheduling.Strategy strategy : ComputeSettings.Scheduling.Strategy.values()) {
                if (strategy.name().equals(normalized)) {
                    return strategy;
                }
            }
        }
        problems.add(new ConfigProblem(
            SCHEDULING_PATH + ".strategy",
            "expected fifo, priority or weighted-fair, got '" + raw + "'; using " + formatStrategy(fallback)
        ));
        return fallback;
    }

    private static OverflowPolicy readOverflow(
        final ConfigurationSection section,
        final String key,
        final OverflowPolicy fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof String text) {
            final String normalized = text.trim().toUpperCase(Locale.ROOT).replace('-', '_');
            for (final OverflowPolicy policy : OverflowPolicy.values()) {
                if (policy.name().equals(normalized)) {
                    return policy;
                }
            }
        }
        problems.add(new ConfigProblem(path, "expected reject or drop-oldest, got '" + raw + "'; using " + formatOverflow(fallback)));
        return fallback;
    }

    private static boolean readBoolean(
        final ConfigurationSection section,
        final String key,
        final boolean fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof Boolean value) {
            return value;
        }
        if (raw instanceof String text) {
            if ("true".equalsIgnoreCase(text.trim())) {
                return true;
            }
            if ("false".equalsIgnoreCase(text.trim())) {
                return false;
            }
        }
        problems.add(new ConfigProblem(path, "expected true or false, got '" + raw + "'; using " + fallback));
        return fallback;
    }

    private static int readInt(
        final ConfigurationSection section,
        final String key,
        final int fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof Double || raw instanceof Float) {
            final double value = ((Number) raw).doubleValue();
            if (!Double.isFinite(value) || value != Math.rint(value)) {
                problems.add(new ConfigProblem(path, "expected a whole number, got " + raw + "; using " + fallback));
                return fallback;
            }
        }
        if (raw instanceof Number number) {
            final long value = number.longValue();
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                problems.add(new ConfigProblem(path, "out of range for a 32 bit integer, got " + raw + "; using " + fallback));
                return fallback;
            }
            return (int) value;
        }
        if (raw instanceof String text) {
            try {
                return Integer.parseInt(text.trim());
            } catch (NumberFormatException _) {
                problems.add(new ConfigProblem(path, "expected a whole number, got '" + text + "'; using " + fallback));
                return fallback;
            }
        }
        problems.add(new ConfigProblem(path, "expected a whole number, got '" + raw + "'; using " + fallback));
        return fallback;
    }

    private static double readDouble(
        final ConfigurationSection section,
        final String key,
        final double fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof Number number) {
            final double value = number.doubleValue();
            if (Double.isFinite(value)) {
                return value;
            }
        }
        if (raw instanceof String text) {
            try {
                final double value = Double.parseDouble(text.trim());
                if (Double.isFinite(value)) {
                    return value;
                }
            } catch (NumberFormatException _) {
                // fall through to the problem below
            }
        }
        problems.add(new ConfigProblem(path, "expected a number, got '" + raw + "'; using " + fallback));
        return fallback;
    }

    private static String readString(
        final ConfigurationSection section,
        final String key,
        final String fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof String text) {
            return text.trim();
        }
        problems.add(new ConfigProblem(path, "expected text, got '" + raw + "'; using '" + fallback + "'"));
        return fallback;
    }

    private static Duration readSeconds(
        final ConfigurationSection section,
        final String key,
        final Duration fallback,
        final boolean allowZero,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Double seconds = readDurationValue(section, key, path, problems);
        if (seconds == null) {
            return fallback;
        }
        if (!allowed(seconds, allowZero)) {
            problems.add(new ConfigProblem(path, wanted(allowZero) + ", got " + seconds + "; using " + formatSeconds(fallback)));
            return fallback;
        }
        return toDuration(seconds, path, fallback, problems);
    }

    private static Duration readMillis(
        final ConfigurationSection section,
        final String key,
        final Duration fallback,
        final boolean allowZero,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Double millis = readDurationValue(section, key, path, problems);
        if (millis == null) {
            return fallback;
        }
        if (!allowed(millis, allowZero)) {
            problems.add(new ConfigProblem(path, wanted(allowZero) + ", got " + millis + "; using " + fallback.toMillis()));
            return fallback;
        }
        if (millis > Long.MAX_VALUE) {
            problems.add(new ConfigProblem(path, "is too large, got " + millis + "; using " + fallback.toMillis()));
            return fallback;
        }
        return Duration.ofMillis(Math.round(millis));
    }

    private static @Nullable Double readDurationValue(
        final ConfigurationSection section,
        final String key,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get(key);
        if (raw == null) {
            return null;
        }
        if (raw instanceof Number number) {
            return number.doubleValue();
        }
        if (raw instanceof String text) {
            try {
                return Double.parseDouble(text.trim());
            } catch (NumberFormatException _) {
                // fall through to the problem below
            }
        }
        problems.add(new ConfigProblem(path, "expected a number, got '" + raw + "'; keeping the default"));
        return null;
    }

    private static boolean allowed(final double value, final boolean allowZero) {
        if (!Double.isFinite(value)) {
            return false;
        }
        return allowZero ? value >= 0.0d : value > 0.0d;
    }

    private static String wanted(final boolean allowZero) {
        return allowZero ? "must not be negative" : "must be positive";
    }

    private static Duration toDuration(
        final double seconds,
        final String path,
        final Duration fallback,
        final List<ConfigProblem> problems
    ) {
        if (seconds > Long.MAX_VALUE / 1000.0d) {
            problems.add(new ConfigProblem(path, "is too large, got " + seconds + " seconds; using " + formatSeconds(fallback)));
            return fallback;
        }
        return Duration.ofMillis(Math.round(seconds * 1000.0d));
    }

    private static @Nullable ConfigurationSection readSection(
        final ConfigurationSection parent,
        final String key,
        final String path,
        final List<ConfigProblem> problems
    ) {
        final Object raw = parent.get(key);
        if (raw == null) {
            return null;
        }
        final ConfigurationSection section = parent.getConfigurationSection(key);
        if (section == null) {
            problems.add(new ConfigProblem(path, "expected a section, got '" + raw + "'; using defaults"));
        }
        return section;
    }

    private static void reportUnknownKeys(final Configuration configuration, final List<ConfigProblem> problems) {
        for (final String key : configuration.getKeys(false)) {
            if (!ROOT_KEYS.contains(key)) {
                problems.add(new ConfigProblem(key, "unknown key, ignored"));
            }
        }
        final ConfigurationSection root = configuration.getConfigurationSection(SECTION);
        if (root == null) {
            return;
        }
        reportUnknownIn(root, SECTION, SETTINGS_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("workers"), WORKERS_PATH, WORKER_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("queue"), QUEUE_PATH, QUEUE_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("scheduling"), SCHEDULING_PATH, SCHEDULING_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("metrics"), METRICS_PATH, METRICS_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("adaptive"), ADAPTIVE_PATH, ADAPTIVE_KEYS, problems);

        final ConfigurationSection lanes = root.getConfigurationSection("lanes");
        reportUnknownIn(lanes, LANES_PATH, LANES_KEYS, problems);
        reportUnknownIn(
            lanes == null ? null : lanes.getConfigurationSection("virtual"),
            LANE_VIRTUAL_PATH,
            LANE_VIRTUAL_KEYS,
            problems
        );

        final ConfigurationSection relief = root.getConfigurationSection("relief");
        reportUnknownIn(relief, RELIEF_PATH, RELIEF_KEYS, problems);
        reportUnknownIn(
            relief == null ? null : relief.getConfigurationSection("consumer"),
            RELIEF_CONSUMER_PATH,
            CONSUMER_KEYS,
            problems
        );
        reportUnknownIn(
            relief == null ? null : relief.getConfigurationSection("trigger"),
            TRIGGER_PATH,
            TRIGGER_KEYS,
            problems
        );

        final ConfigurationSection consumers = root.getConfigurationSection("consumers");
        if (consumers == null) {
            return;
        }
        for (final String key : consumers.getKeys(false)) {
            if (CONSUMERS_KEYS.contains(key)) {
                if (DEFAULT_CONSUMER.equals(key)) {
                    reportUnknownIn(consumers.getConfigurationSection(key), CONSUMERS_PATH + "." + key, CONSUMER_KEYS, problems);
                }
                continue;
            }
            final ConfigurationSection consumer = consumers.getConfigurationSection(key);
            if (consumer == null) {
                problems.add(new ConfigProblem(CONSUMERS_PATH + "." + key, "expected a consumer section, ignored"));
                continue;
            }
            reportUnknownIn(consumer, CONSUMERS_PATH + "." + key, CONSUMER_KEYS, problems);
        }
    }

    private static void reportUnknownIn(
        final ConfigurationSection section,
        final String path,
        final Set<String> known,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return;
        }
        for (final String key : section.getKeys(false)) {
            if (!known.contains(key)) {
                problems.add(new ConfigProblem(path + "." + key, "unknown key, ignored"));
            }
        }
    }
}
