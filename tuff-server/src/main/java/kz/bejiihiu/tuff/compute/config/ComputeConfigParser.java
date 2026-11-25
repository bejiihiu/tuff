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
import java.util.Set;
import java.util.TreeMap;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
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

    private static final String WORKERS_PATH = SECTION + ".workers";
    private static final String QUEUE_PATH = SECTION + ".queue";
    private static final String SCHEDULING_PATH = SECTION + ".scheduling";
    private static final String CONSUMERS_PATH = SECTION + ".consumers";
    private static final String METRICS_PATH = SECTION + ".metrics";
    private static final String ADAPTIVE_PATH = SECTION + ".adaptive";

    private static final String DEFAULT_CONSUMER = "default";
    private static final String ALLOW_DYNAMIC_REGISTRATION = "allow-dynamic-registration";

    private static final Set<String> ROOT_KEYS = Set.of(SECTION);
    private static final Set<String> SETTINGS_KEYS = Set.of(
        "enabled", "workers", "queue", "scheduling", "consumers", "metrics", "adaptive", "shutdown-grace-seconds"
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

    private ComputeConfigParser() {
    }

    /**
     * Reads and parses a configuration file as UTF-8.
     *
     * @param file     file to read
     * @param problems problem sink, appended to in place
     * @return the parsed settings, never {@code null}
     * @throws IOException                   if the file cannot be read
     * @throws InvalidConfigurationException if the file is not valid YAML
     */
    static ComputeSettings read(final Path file, final List<ConfigProblem> problems) throws IOException, InvalidConfigurationException {
        final YamlConfiguration configuration = new YamlConfiguration();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            configuration.load(reader);
        }
        return parse(configuration, problems);
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
        final Duration grace = readSeconds(root, "shutdown-grace-seconds", defaults.shutdownGracePeriod(), true, SECTION + ".shutdown-grace-seconds", problems);

        try {
            return new ComputeSettings(enabled, workers, queue, scheduling, consumers, metrics, adaptive, grace);
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

        flat.put(SECTION + ".shutdown-grace-seconds", formatSeconds(settings.shutdownGracePeriod()));
        return flat;
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
        final int maxConcurrency = readInt(section, "max-concurrency", fallback.maxConcurrency(), ADAPTIVE_PATH + ".max-concurrency", problems);
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
