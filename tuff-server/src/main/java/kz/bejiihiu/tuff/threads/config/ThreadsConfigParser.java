package kz.bejiihiu.tuff.threads.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import kz.bejiihiu.tuff.compute.config.ConfigProblem;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.kairos.KairosGate;
import kz.bejiihiu.tuff.threads.kairos.KairosKernel;
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
    private static final String COMPUTE_POOL_PATH = POOLS_PATH + ".compute";
    private static final String TICK_PRESSURE_PATH = ASYNC_COMPUTE_PATH + ".tick-pressure";
    private static final String METRICS_PATH = SECTION + ".metrics";

    private static final Set<String> SETTINGS_KEYS = Set.of(
        "enabled", "resources", "lifecycle", "region-execution", "async-compute", "execution-pools", "metrics"
    );
    private static final Set<String> RESOURCE_KEYS = Set.of(
        "cpu-aware", "reserve-cores", "max-concurrent-tasks", "load-shedding"
    );
    private static final Set<String> LIFECYCLE_KEYS = Set.of("centralized-thread-factory", "monitor-threads");
    private static final Set<String> REGION_KEYS = Set.of(
        "managed", "preserve-folia-scheduling", "scheduler", "threads", "grid-exponent", "kairos-unstable", "kairos"
    );
    private static final String KAIROS_PATH = REGION_PATH + ".kairos";
    private static final Set<String> KAIROS_KEYS = Set.of(
        "elastic", "max-threads", "keep-alive-seconds", "spawn-rate-per-second",
        "catch-up", "steal", "affinity", "lease", "metrics"
    );
    private static final String CATCH_UP_PATH = KAIROS_PATH + ".catch-up";
    private static final Set<String> CATCH_UP_KEYS = Set.of("cap-ticks", "budget-millis");
    private static final String STEAL_PATH = KAIROS_PATH + ".steal";
    private static final Set<String> STEAL_KEYS = Set.of("threshold-millis");
    private static final String AFFINITY_PATH = KAIROS_PATH + ".affinity";
    private static final Set<String> AFFINITY_KEYS = Set.of("local-queue-size");
    private static final String LEASE_PATH = KAIROS_PATH + ".lease";
    private static final Set<String> LEASE_KEYS = Set.of("timeout-seconds");
    private static final String KAIROS_METRICS_PATH = KAIROS_PATH + ".metrics";
    private static final Set<String> KAIROS_METRICS_KEYS = Set.of("jfr");
    private static final Set<String> ASYNC_COMPUTE_KEYS = Set.of("managed", "max-threads", "tick-pressure");
    private static final Set<String> TICK_PRESSURE_KEYS = Set.of(
        "enabled",
        "enter-lag-fraction",
        "enter-min-tps",
        "exit-min-tps",
        "exit-windows",
        "cooldown-seconds",
        "max-reserve-cores",
        "step-cores"
    );
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
        // a read that has no kairos block still has an answer for the scheduler: the stock behaviour
        KairosGate.kernel(KairosKernel.defaults());
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
        flat.put(RESOURCES_PATH + ".load-shedding", Boolean.toString(resources.loadShedding()));

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
        final ThreadManagementSettings.RegionExecution.Kairos kairos = region.kairos();
        flat.put(KAIROS_PATH + ".elastic", Boolean.toString(kairos.elastic()));
        flat.put(KAIROS_PATH + ".max-threads", Integer.toString(kairos.maxThreads()));
        flat.put(KAIROS_PATH + ".keep-alive-seconds", Integer.toString(kairos.keepAliveSeconds()));
        flat.put(KAIROS_PATH + ".spawn-rate-per-second", Integer.toString(kairos.spawnRatePerSecond()));

        final ThreadManagementSettings.AsyncCompute asyncCompute = settings.asyncCompute();
        flat.put(ASYNC_COMPUTE_PATH + ".managed", Boolean.toString(asyncCompute.managed()));
        flat.put(ASYNC_COMPUTE_PATH + ".max-threads", Integer.toString(asyncCompute.maxThreads()));
        final ThreadManagementSettings.AsyncCompute.TickPressure pressure = asyncCompute.tickPressure();
        flat.put(TICK_PRESSURE_PATH + ".enabled", Boolean.toString(pressure.enabled()));
        flat.put(TICK_PRESSURE_PATH + ".enter-lag-fraction", Double.toString(pressure.enterLagFraction()));
        flat.put(TICK_PRESSURE_PATH + ".enter-min-tps", Double.toString(pressure.enterMinTps()));
        flat.put(TICK_PRESSURE_PATH + ".exit-min-tps", Double.toString(pressure.exitMinTps()));
        flat.put(TICK_PRESSURE_PATH + ".exit-windows", Integer.toString(pressure.exitWindows()));
        flat.put(TICK_PRESSURE_PATH + ".cooldown-seconds", Integer.toString(pressure.cooldownSeconds()));
        flat.put(TICK_PRESSURE_PATH + ".max-reserve-cores", Integer.toString(pressure.maxReserveCores()));
        flat.put(TICK_PRESSURE_PATH + ".step-cores", Integer.toString(pressure.stepCores()));

        final ThreadManagementSettings.Pools pools = settings.executionPools();
        flat.put(NETWORK_PATH + ".managed", Boolean.toString(pools.network().managed()));
        flat.put(NETWORK_PATH + ".preserve-specialization", Boolean.toString(pools.network().preserveSpecialization()));
        flat.put(CHUNK_PATH + ".managed", Boolean.toString(pools.chunkSystem().managed()));
        flat.put(CHUNK_PATH + ".preserve-specialization", Boolean.toString(pools.chunkSystem().preserveSpecialization()));
        flat.put(COMPUTE_POOL_PATH + ".managed", Boolean.toString(pools.compute().managed()));
        flat.put(COMPUTE_POOL_PATH + ".preserve-specialization", Boolean.toString(pools.compute().preserveSpecialization()));

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
        final boolean loadShedding = readBoolean(
            section, "load-shedding", fallback.loadShedding(), RESOURCES_PATH + ".load-shedding", problems
        );
        try {
            return new ThreadManagementSettings.Resources(cpuAware, reserve, maxTasks, loadShedding);
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
        final ThreadManagementSettings.RegionExecution.Kairos kairosLimits = parseKairos(
            readSection(section, "kairos", KAIROS_PATH, problems), fallback.kairos(), problems
        );
        try {
            return new ThreadManagementSettings.RegionExecution(
                managed, preserve, scheduler, threads, gridExponent, kairos, kairosLimits
            );
        } catch (final IllegalArgumentException | NullPointerException ex) {
            problems.add(new ConfigProblem(REGION_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ThreadManagementSettings.RegionExecution.Kairos parseKairos(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.RegionExecution.Kairos fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            KairosGate.kernel(KairosKernel.defaults());
            return fallback;
        }
        final boolean elastic = readBoolean(section, "elastic", fallback.elastic(), KAIROS_PATH + ".elastic", problems);
        final int maxThreads = readInt(
            section, "max-threads", fallback.maxThreads(), KAIROS_PATH + ".max-threads", problems
        );
        final int keepAlive = readInt(
            section, "keep-alive-seconds", fallback.keepAliveSeconds(), KAIROS_PATH + ".keep-alive-seconds", problems
        );
        final int spawnRate = readInt(
            section, "spawn-rate-per-second", fallback.spawnRatePerSecond(),
            KAIROS_PATH + ".spawn-rate-per-second", problems
        );
        // the tick level knobs are published to the scheduler's opt-in gate instead of travelling
        // through the settings record: that record belongs to the thread layer and is shared with
        // pools that have no idea what a tick is. the gate is already the value that crosses this
        // window, so the knobs arrive at the scheduler start with the rest of the kairos block
        KairosGate.kernel(parseKernel(section, problems));
        try {
            return new ThreadManagementSettings.RegionExecution.Kairos(elastic, maxThreads, keepAlive, spawnRate);
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(KAIROS_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    /**
     * Reads the knobs that act on a single tick or permit and publishes them to the scheduler.
     *
     * <p>A value outside its range is reported and replaced by the stock behaviour rather than by the
     * neighbouring field, so one bad number cannot quietly change two behaviours at once.
     *
     * @param kairos   the {@code kairos} section
     * @param problems problem sink, appended to in place
     * @return the knobs to publish, never {@code null}
     */
    private static KairosKernel parseKernel(final ConfigurationSection kairos, final List<ConfigProblem> problems) {
        final KairosKernel defaults = KairosKernel.defaults();
        final ConfigurationSection catchUp = readSection(kairos, "catch-up", CATCH_UP_PATH, problems);
        final ConfigurationSection steal = readSection(kairos, "steal", STEAL_PATH, problems);
        final ConfigurationSection affinity = readSection(kairos, "affinity", AFFINITY_PATH, problems);
        final ConfigurationSection lease = readSection(kairos, "lease", LEASE_PATH, problems);
        final ConfigurationSection metrics = readSection(kairos, "metrics", KAIROS_METRICS_PATH, problems);
        final int cap = readIntIn(
            catchUp, "cap-ticks", defaults.catchUpCapTicks(), CATCH_UP_PATH + ".cap-ticks", problems
        );
        final int budget = readIntIn(
            catchUp, "budget-millis", defaults.catchUpBudgetMillis(), CATCH_UP_PATH + ".budget-millis", problems
        );
        final int stealThreshold = readIntIn(
            steal, "threshold-millis", defaults.stealThresholdMillis(), STEAL_PATH + ".threshold-millis", problems
        );
        final int queueSize = readIntIn(
            affinity, "local-queue-size", defaults.affinityQueueSize(), AFFINITY_PATH + ".local-queue-size", problems
        );
        final int leaseSeconds = readIntIn(
            lease, "timeout-seconds", defaults.leaseTimeoutMillis() / 1000, LEASE_PATH + ".timeout-seconds", problems
        );
        final boolean jfr = metrics != null && readBoolean(
            metrics, "jfr", defaults.jfr(), KAIROS_METRICS_PATH + ".jfr", problems
        );
        try {
            return new KairosKernel(cap, budget, queueSize, stealThreshold, leaseSeconds * 1000, jfr);
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(CATCH_UP_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return defaults;
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
        final ThreadManagementSettings.AsyncCompute.TickPressure pressure = parseTickPressure(
            readSection(section, "tick-pressure", TICK_PRESSURE_PATH, problems), fallback.tickPressure(), problems
        );
        try {
            return new ThreadManagementSettings.AsyncCompute(managed, maxThreads, pressure);
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(ASYNC_COMPUTE_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
            return fallback;
        }
    }

    private static ThreadManagementSettings.AsyncCompute.TickPressure parseTickPressure(
        final @Nullable ConfigurationSection section,
        final ThreadManagementSettings.AsyncCompute.TickPressure fallback,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        final boolean enabled = readBoolean(
            section, "enabled", fallback.enabled(), TICK_PRESSURE_PATH + ".enabled", problems
        );
        final double lagFraction = readDouble(
            section, "enter-lag-fraction", fallback.enterLagFraction(),
            TICK_PRESSURE_PATH + ".enter-lag-fraction", problems
        );
        final double enterMinTps = readDouble(
            section, "enter-min-tps", fallback.enterMinTps(), TICK_PRESSURE_PATH + ".enter-min-tps", problems
        );
        final double exitMinTps = readDouble(
            section, "exit-min-tps", fallback.exitMinTps(), TICK_PRESSURE_PATH + ".exit-min-tps", problems
        );
        final int exitWindows = readInt(
            section, "exit-windows", fallback.exitWindows(), TICK_PRESSURE_PATH + ".exit-windows", problems
        );
        final int cooldown = readInt(
            section, "cooldown-seconds", fallback.cooldownSeconds(), TICK_PRESSURE_PATH + ".cooldown-seconds", problems
        );
        final int maxReserve = readInt(
            section, "max-reserve-cores", fallback.maxReserveCores(), TICK_PRESSURE_PATH + ".max-reserve-cores", problems
        );
        final int step = readInt(
            section, "step-cores", fallback.stepCores(), TICK_PRESSURE_PATH + ".step-cores", problems
        );
        try {
            return new ThreadManagementSettings.AsyncCompute.TickPressure(
                enabled, lagFraction, enterMinTps, exitMinTps, exitWindows, cooldown, maxReserve, step
            );
        } catch (final IllegalArgumentException ex) {
            problems.add(new ConfigProblem(TICK_PRESSURE_PATH, "rejected (" + ex.getMessage() + "), using defaults"));
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
        final ThreadManagementSettings.PoolSettings compute = parsePool(
            readSection(section, "compute", COMPUTE_POOL_PATH, problems), fallback.compute(), COMPUTE_POOL_PATH, problems
        );
        return new ThreadManagementSettings.Pools(network, chunk, compute);
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

    /**
     * Reads a whole number from an optional section.
     *
     * @param section  section that holds the key, or {@code null} when the operator did not write it
     * @param key      key inside the section
     * @param fallback value to use when the key or the section is missing
     * @param path     dotted path for the problem message
     * @param problems problem sink, appended to in place
     * @return the value, or the fallback
     */
    private static int readIntIn(
        final @Nullable ConfigurationSection section,
        final String key,
        final int fallback,
        final String path,
        final List<ConfigProblem> problems
    ) {
        if (section == null) {
            return fallback;
        }
        return readInt(section, key, fallback, path, problems);
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
                // falls through to the problem below
            }
        }
        problems.add(new ConfigProblem(path, "expected a number, got '" + raw + "'; using " + fallback));
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
        final ConfigurationSection region = root.getConfigurationSection("region-execution");
        if (region != null) {
            final ConfigurationSection kairos = region.getConfigurationSection("kairos");
            reportUnknownIn(kairos, KAIROS_PATH, KAIROS_KEYS, problems);
            if (kairos != null) {
                reportUnknownIn(kairos.getConfigurationSection("catch-up"), CATCH_UP_PATH, CATCH_UP_KEYS, problems);
                reportUnknownIn(kairos.getConfigurationSection("steal"), STEAL_PATH, STEAL_KEYS, problems);
                reportUnknownIn(kairos.getConfigurationSection("affinity"), AFFINITY_PATH, AFFINITY_KEYS, problems);
                reportUnknownIn(kairos.getConfigurationSection("lease"), LEASE_PATH, LEASE_KEYS, problems);
                reportUnknownIn(
                    kairos.getConfigurationSection("metrics"), KAIROS_METRICS_PATH, KAIROS_METRICS_KEYS, problems
                );
            }
        }
        reportUnknownIn(root.getConfigurationSection("async-compute"), ASYNC_COMPUTE_PATH, ASYNC_COMPUTE_KEYS, problems);
        final ConfigurationSection asyncCompute = root.getConfigurationSection("async-compute");
        if (asyncCompute != null) {
            reportUnknownIn(asyncCompute.getConfigurationSection("tick-pressure"), TICK_PRESSURE_PATH, TICK_PRESSURE_KEYS, problems);
        }
        reportUnknownIn(root.getConfigurationSection("metrics"), METRICS_PATH, METRICS_KEYS, problems);

        final ConfigurationSection pools = root.getConfigurationSection("execution-pools");
        if (pools == null) {
            return;
        }
        reportUnknownIn(pools, POOLS_PATH, Set.of("network", "chunk-system", "compute"), problems);
        reportUnknownIn(pools.getConfigurationSection("network"), NETWORK_PATH, POOL_KEYS, problems);
        reportUnknownIn(pools.getConfigurationSection("chunk-system"), CHUNK_PATH, POOL_KEYS, problems);
        reportUnknownIn(pools.getConfigurationSection("compute"), COMPUTE_POOL_PATH, POOL_KEYS, problems);
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
