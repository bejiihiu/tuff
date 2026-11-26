package kz.bejiihiu.tuff.threads.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.bukkit.configuration.Configuration;
import org.bukkit.configuration.ConfigurationSection;
import org.jspecify.annotations.Nullable;

/**
 * Translates the {@code thread-management} section of {@code config/tuff.yml} into
 * {@link ThreadManagementSettings}.
 *
 * <p>The reader never touches the file: it is handed a loaded {@link Configuration} and returns a
 * settings tree. A key that cannot be read keeps its default and is reported; a section whose values
 * contradict each other is validated by its own record, and the whole section falls back to defaults
 * with one problem instead of half a section silently surviving. That is the same trade the compute
 * configuration makes, for the same reason: a server that refuses to boot over one typo is worse than a
 * server that says which typo it ignored.
 *
 * <p>Enums are written in kebab case ({@code work-stealing} for {@code WORK_STEALING}), counts use
 * {@code -1} for "let the owner decide" and every boolean is literal {@code true}/{@code false}.
 */
final class ThreadsConfigParser {

    static final String SECTION = "thread-management";

    private static final String RESOURCES_PATH = SECTION + ".resources";
    private static final String LIFECYCLE_PATH = SECTION + ".lifecycle";
    private static final String REGION_PATH = SECTION + ".region-execution";
    private static final String ASYNC_COMPUTE_PATH = SECTION + ".async-compute";
    private static final String POOLS_PATH = SECTION + ".execution-pools";
    private static final String NETWORK_PATH = POOLS_PATH + ".network";
    private static final String CHUNK_PATH = POOLS_PATH + ".chunk-system";
    private static final String METRICS_PATH = SECTION + ".metrics";

    private static final Set<String> SETTINGS_KEYS = Set.of(
        "enabled", "resources", "lifecycle", "region-execution", "async-compute", "execution-pools", "metrics"
    );
    private static final Set<String> RESOURCE_KEYS = Set.of("cpu-aware", "reserve-cores", "max-concurrent-tasks");
    private static final Set<String> LIFECYCLE_KEYS = Set.of("centralized-thread-factory", "monitor-threads");
    private static final Set<String> REGION_KEYS = Set.of(
        "managed", "preserve-folia-scheduling", "scheduler", "threads", "grid-exponent", "kairos-unstable"
    );
    private static final Set<String> ASYNC_COMPUTE_KEYS = Set.of("managed", "max-threads");
    private static final Set<String> POOL_KEYS = Set.of("managed", "preserve-specialization");
    private static final Set<String> METRICS_KEYS = Set.of("enabled", "per-pool", "per-thread");

    private ThreadsConfigParser() {
    }

    /**
     * Parses the section out of a loaded configuration tree.
     *
     * @param configuration loaded YAML
     * @param problems      problem sink, appended to in place
     * @return the settings, never {@code null}
     */
    static ThreadManagementSettings parse(final Configuration configuration, final List<ConfigProblem> problems) {
        final ThreadManagementSettings defaults = ThreadManagementSettings.defaults();
        final ConfigurationSection root = configuration.getConfigurationSection(SECTION);
        if (root == null) {
            problems.add(new ConfigProblem(SECTION, "section is missing, using the observe-only defaults"));
            return defaults;
        }
        reportUnknownKeys(root, problems);

        final boolean enabled = readBoolean(root, "enabled", defaults.enabled(), SECTION + ".enabled", problems);
        final ThreadManagementSettings.Resources resources = parseResources(
            readSection(root, "resources", RESOURCES_PATH, problems), defaults.resources(), problems
        );
        final ThreadManagementSettings.Lifecycle lifecycle = parseLifecycle(
            readSection(root, "lifecycle", LIFECYCLE_PATH, problems), defaults.lifecycle(), problems
        );
        final ThreadManagementSettings.RegionExecution region = parseRegion(
            readSection(root, "region-execution", REGION_PATH, problems), defaults.regionExecution(), problems
        );
        final ThreadManagementSettings.AsyncCompute asyncCompute = parseAsyncCompute(
            readSection(root, "async-compute", ASYNC_COMPUTE_PATH, problems), defaults.asyncCompute(), problems
        );
        final ThreadManagementSettings.Pools pools = parsePools(
            readSection(root, "execution-pools", POOLS_PATH, problems), defaults.executionPools(), problems
        );
        final ThreadManagementSettings.Metrics metrics = parseMetrics(
            readSection(root, "metrics", METRICS_PATH, problems), defaults.metrics(), problems
        );
        return new ThreadManagementSettings(enabled, resources, lifecycle, region, asyncCompute, pools, metrics);
    }

    /**
     * Renders settings as dotted keys of {@code config/tuff.yml}.
     *
     * <p>Used by tests and by anything that wants to compare two snapshots; the section has no live
     * reload, so the runtime itself does not flatten it.
     *
     * @param settings settings to render
     * @return ordered map of dotted key to display value
     */
    static Map<String, String> flatten(final ThreadManagementSettings settings) {
        final Map<String, String> flat = new LinkedHashMap<>();
        flat.put(SECTION + ".enabled", Boolean.toString(settings.enabled()));

        final ThreadManagementSettings.Resources resources = settings.resources();
        flat.put(RESOURCES_PATH + ".cpu-aware", Boolean.toString(resources.cpuAware()));
        flat.put(RESOURCES_PATH + ".reserve-cores", Integer.toString(resources.reserveCores()));
        flat.put(RESOURCES_PATH + ".max-concurrent-tasks", Integer.toString(resources.maxConcurrentTasks()));

        final ThreadManagementSettings.Lifecycle lifecycle = settings.lifecycle();
        flat.put(LIFECYCLE_PATH + ".centralized-thread-factory", Boolean.toString(lifecycle.centralizedThreadFactory()));
        flat.put(LIFECYCLE_PATH + ".monitor-threads", Boolean.toString(lifecycle.monitorThreads()));

        final ThreadManagementSettings.RegionExecution region = settings.regionExecution();
        flat.put(REGION_PATH + ".managed", Boolean.toString(region.managed()));
        flat.put(REGION_PATH + ".preserve-folia-scheduling", Boolean.toString(region.preserveFoliaScheduling()));
        flat.put(REGION_PATH + ".scheduler", formatScheduler(region.scheduler()));
        flat.put(REGION_PATH + ".threads", Integer.toString(region.threads()));
        flat.put(REGION_PATH + ".grid-exponent", Integer.toString(region.gridExponent()));
        flat.put(REGION_PATH + ".kairos-unstable", Boolean.toString(region.kairosUnstable()));

        final ThreadManagementSettings.AsyncCompute asyncCompute = settings.asyncCompute();
        flat.put(ASYNC_COMPUTE_PATH + ".managed", Boolean.toString(asyncCompute.managed()));
        flat.put(ASYNC_COMPUTE_PATH + ".max-threads", Integer.toString(asyncCompute.maxThreads()));

        final ThreadManagementSettings.Pools pools = settings.executionPools();
        flat.put(NETWORK_PATH + ".managed", Boolean.toString(pools.network().managed()));
        flat.put(NETWORK_PATH + ".preserve-specialization", Boolean.toString(pools.network().preserveSpecialization()));
        flat.put(CHUNK_PATH + ".managed", Boolean.toString(pools.chunkSystem().managed()));
        flat.put(CHUNK_PATH + ".preserve-specialization", Boolean.toString(pools.chunkSystem().preserveSpecialization()));

        final ThreadManagementSettings.Metrics metrics = settings.metrics();
        flat.put(METRICS_PATH + ".enabled", Boolean.toString(metrics.enabled()));
        flat.put(METRICS_PATH + ".per-pool", Boolean.toString(metrics.perPool()));
        flat.put(METRICS_PATH + ".per-thread", Boolean.toString(metrics.perThread()));
        return flat;
    }

    private static ThreadManagementSettings.Resources parseResources(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.Resources fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean cpuAware = readBoolean(section, "cpu-aware", fallback.cpuAware(), RESOURCES_PATH + ".cpu-aware", problems);
        final int reserve = readInt(section, "reserve-cores", fallback.reserveCores(), RESOURCES_PATH + ".reserve-cores", problems);
        final int maxTasks = readInt(
            section, "max-concurrent-tasks", fallback.maxConcurrentTasks(), RESOURCES_PATH + ".max-concurrent-tasks", problems
        );
        try {
            return new ThreadManagementSettings.Resources(cpuAware, reserve, maxTasks);
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(RESOURCES_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ThreadManagementSettings.Lifecycle parseLifecycle(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.Lifecycle fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean factory = readBoolean(
            section, "centralized-thread-factory", fallback.centralizedThreadFactory(),
            LIFECYCLE_PATH + ".centralized-thread-factory", problems
        );
        final boolean monitor = readBoolean(
            section, "monitor-threads", fallback.monitorThreads(), LIFECYCLE_PATH + ".monitor-threads", problems
        );
        return new ThreadManagementSettings.Lifecycle(factory, monitor);
    }

    private static ThreadManagementSettings.RegionExecution parseRegion(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.RegionExecution fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean managed = readBoolean(section, "managed", fallback.managed(), REGION_PATH + ".managed", problems);
        final boolean preserve = readBoolean(
            section, "preserve-folia-scheduling", fallback.preserveFoliaScheduling(),
            REGION_PATH + ".preserve-folia-scheduling", problems
        );
        final ThreadManagementSettings.RegionScheduler scheduler = readScheduler(
            section, fallback.scheduler(), problems
        );
        final int threads = readInt(section, "threads", fallback.threads(), REGION_PATH + ".threads", problems);
        final int gridExponent = readInt(
            section, "grid-exponent", fallback.gridExponent(), REGION_PATH + ".grid-exponent", problems
        );
        final boolean kairos = readBoolean(
            section, "kairos-unstable", fallback.kairosUnstable(), REGION_PATH + ".kairos-unstable", problems
        );
        try {
            return new ThreadManagementSettings.RegionExecution(managed, preserve, scheduler, threads, gridExponent, kairos);
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(REGION_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ThreadManagementSettings.AsyncCompute parseAsyncCompute(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.AsyncCompute fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean managed = readBoolean(section, "managed", fallback.managed(), ASYNC_COMPUTE_PATH + ".managed", problems);
        final int maxThreads = readInt(
            section, "max-threads", fallback.maxThreads(), ASYNC_COMPUTE_PATH + ".max-threads", problems
        );
        try {
            return new ThreadManagementSettings.AsyncCompute(managed, maxThreads);
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(ASYNC_COMPUTE_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ThreadManagementSettings.Pools parsePools(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.Pools fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final ThreadManagementSettings.PoolSettings network = parsePool(
            readSection(section, "network", NETWORK_PATH, problems), fallback.network(), NETWORK_PATH, problems
        );
        final ThreadManagementSettings.PoolSettings chunk = parsePool(
            readSection(section, "chunk-system", CHUNK_PATH, problems), fallback.chunkSystem(), CHUNK_PATH, problems
        );
        return new ThreadManagementSettings.Pools(network, chunk);
    }

    private static ThreadManagementSettings.PoolSettings parsePool(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.PoolSettings fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean managed = readBoolean(section, "managed", fallback.managed(), path + ".managed", problems);
        final boolean preserve = readBoolean(
            section, "preserve-specialization", fallback.preserveSpecialization(), path + ".preserve-specialization", problems
        );
        try {
            return new ThreadManagementSettings.PoolSettings(managed, preserve);
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(path, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ThreadManagementSettings.Metrics parseMetrics(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.Metrics fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(section, "enabled", fallback.enabled(), METRICS_PATH + ".enabled", problems);
        final boolean perPool = readBoolean(section, "per-pool", fallback.perPool(), METRICS_PATH + ".per-pool", problems);
        final boolean perThread = readBoolean(section, "per-thread", fallback.perThread(), METRICS_PATH + ".per-thread", problems);
        return new ThreadManagementSettings.Metrics(enabled, perPool, perThread);
    }

    private static String formatScheduler(final ThreadManagementSettings.RegionScheduler scheduler) {
        return scheduler.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    private static ThreadManagementSettings.RegionScheduler readScheduler(
        final ConfigurationSection section,
        final ThreadManagementSettings.RegionScheduler fallback,
        final List<ConfigProblem> problems
    ) {
        final Object raw = section.get("scheduler");
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof String text) {
            final String normalized = text.trim().toUpperCase(Locale.ROOT).replace('-', '_');
            for (final ThreadManagementSettings.RegionScheduler scheduler : ThreadManagementSettings.RegionScheduler.values()) {
                if (scheduler.name().equals(normalized)) {
                    return scheduler;
                }
            }
        }
        problems.add(new ConfigProblem(
            REGION_PATH + ".scheduler",
            "expected edf, work-stealing or kairos, got '" + raw + "'; using " + formatScheduler(fallback)
        ));
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

    private static void reportUnknownKeys(final ConfigurationSection root, final List<ConfigProblem> problems) {
        reportUnknownIn(root, SECTION, SETTINGS_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("resources"), RESOURCES_PATH, RESOURCE_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("lifecycle"), LIFECYCLE_PATH, LIFECYCLE_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("region-execution"), REGION_PATH, REGION_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("async-compute"), ASYNC_COMPUTE_PATH, ASYNC_COMPUTE_KEYS, problems);
        reportUnknownIn(root.getConfigurationSection("metrics"), METRICS_PATH, METRICS_KEYS, problems);

        final ConfigurationSection pools = root.getConfigurationSection("execution-pools");
        if (pools == null) {
            return;
        }
        reportUnknownIn(pools, POOLS_PATH, Set.of("network", "chunk-system"), problems);
        reportUnknownIn(pools.getConfigurationSection("network"), NETWORK_PATH, POOL_KEYS, problems);
        reportUnknownIn(pools.getConfigurationSection("chunk-system"), CHUNK_PATH, POOL_KEYS, problems);
    }

    private static void reportUnknownIn(
        final @Nullable ConfigurationSection section,
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
