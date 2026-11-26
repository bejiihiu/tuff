package kz.bejiihiu.tuff.threads.internal.impl.topology;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;

/**
 * Reads the processor topology once and exposes it as a {@link TopologyView}.
 *
 * <p>The only count that participates in sizing is {@link #availableProcessors()}. On Linux that is
 * already cgroup and cpuset aware, which is exactly what we want; the kernel-wide {@code possible}
 * cpumask is read too but only lands in {@link #totalCores()} for logs, because feeding it into a
 * sizing formula is how a container with eight allowed CPUs ends up with sixteen tick threads.
 *
 * <p>Nothing is cached between managers: a manager reads the topology at start and keeps the resulting
 * {@link TopologySnapshot} for its whole life. A changed topology is a restart, not a hot path.
 *
 * <p>All parsing is exposed through package private helpers so the tests can drive real sysfs trees
 * instead of mocking the filesystem.
 */
public final class TopologyProbe implements TopologyView {

    /**
     * Directory names that hold per-CPU topology, for example {@code cpu7}.
     */
    private static final Pattern CPU_DIRECTORY = Pattern.compile("cpu\\d+");

    /**
     * Placeholder a cgroup writes instead of a quota.
     */
    private static final String NO_QUOTA = "max";

    private final TopologySnapshot snapshot;

    private TopologyProbe(final TopologySnapshot snapshot) {
        this.snapshot = snapshot;
    }

    /**
     * Reads the topology of the running machine.
     *
     * <p>Uses {@code /sys} on Linux and falls back to {@link Runtime#availableProcessors()} everywhere
     * else. Never throws: a missing sysfs tree simply produces a runtime-only snapshot.
     *
     * @return the probe
     */
    public static TopologyProbe detect() {
        return read(Path.of("/sys"), Runtime.getRuntime().availableProcessors());
    }

    /**
     * Reads a topology rooted at a given sysfs directory.
     *
     * <p>The root is a parameter instead of a constant so tests can build a fake {@code /sys} tree and
     * exercise the real parser. A root without {@code devices/system/cpu} is treated as a platform with
     * no sysfs, which is what Windows looks like.
     *
     * @param sysRoot             root of the sysfs tree, usually {@code /sys}
     * @param availableProcessors processors the JVM may use, at least one
     * @return the probe
     * @throws NullPointerException     if {@code sysRoot} is {@code null}
     * @throws IllegalArgumentException if {@code availableProcessors} is below one
     */
    public static TopologyProbe read(final Path sysRoot, final int availableProcessors) {
        Objects.requireNonNull(sysRoot, "sysRoot");
        if (availableProcessors < 1) {
            throw new IllegalArgumentException(
                "availableProcessors must be at least 1, got " + availableProcessors
            );
        }
        return new TopologyProbe(scan(sysRoot, availableProcessors));
    }

    /**
     * Wraps an already read snapshot.
     *
     * @param snapshot the reading
     * @return the probe
     * @throws NullPointerException if {@code snapshot} is {@code null}
     */
    public static TopologyProbe of(final TopologySnapshot snapshot) {
        return new TopologyProbe(Objects.requireNonNull(snapshot, "snapshot"));
    }

    /**
     * Returns the raw reading behind this view.
     *
     * @return the snapshot
     */
    public TopologySnapshot snapshot() {
        return this.snapshot;
    }

    @Override
    public int availableProcessors() {
        return this.snapshot.availableProcessors();
    }

    @Override
    public int totalCores() {
        final int possible = this.snapshot.possibleCpus();
        return possible > 0 ? possible : this.snapshot.availableProcessors();
    }

    @Override
    public boolean numaAvailable() {
        return this.snapshot.numaAvailable();
    }

    @Override
    public int numaNodeCount() {
        return this.snapshot.numaNodeCount();
    }

    /**
     * Reads a whole sysfs tree.
     *
     * @param sysRoot             sysfs root
     * @param availableProcessors processors the JVM may use
     * @return the snapshot
     */
    private static TopologySnapshot scan(final Path sysRoot, final int availableProcessors) {
        final Path cpuRoot = sysRoot.resolve("devices").resolve("system").resolve("cpu");
        if (!Files.isDirectory(cpuRoot)) {
            return TopologySnapshot.runtimeOnly(availableProcessors);
        }

        final int possibleCpus = countCpuList(cpuRoot.resolve("possible"));
        final CoreCounts cores = readCoreCounts(cpuRoot);
        final Path cgroupRoot = sysRoot.resolve("fs").resolve("cgroup");
        final int quota = readCgroupQuota(cgroupRoot);
        final int cpuset = readCgroupCpuset(cgroupRoot);
        final int nodes = countNumaNodes(sysRoot.resolve("devices").resolve("system").resolve("node"));

        return new TopologySnapshot(
            availableProcessors,
            possibleCpus,
            cores.physical(),
            cores.logical(),
            cores.siblingsPerCore(),
            quota,
            cpuset,
            Math.max(1, nodes),
            nodes > 1,
            "linux-sysfs"
        );
    }

    /**
     * Counts logical CPUs and distinct sibling groups.
     *
     * <p>Physical cores are the number of distinct {@code thread_siblings_list} values, not
     * {@code cores / 2}: that division is wrong on any machine that disables hyper threading on some
     * cores only.
     *
     * @param cpuRoot sysfs CPU directory
     * @return the counts, with {@code -1} for anything unreadable
     */
    private static CoreCounts readCoreCounts(final Path cpuRoot) {
        final Set<String> siblings = new LinkedHashSet<>();
        int logical = 0;
        try (Stream<Path> entries = Files.list(cpuRoot)) {
            for (final Path entry : entries.toList()) {
                if (!CPU_DIRECTORY.matcher(entry.getFileName().toString()).matches()) {
                    continue;
                }
                final String list = readTrimmed(entry.resolve("topology").resolve("thread_siblings_list"));
                if (list.isEmpty()) {
                    continue;
                }
                logical++;
                siblings.add(list);
            }
        } catch (IOException _) {
            return new CoreCounts(-1, -1, -1);
        }
        if (logical == 0 || siblings.isEmpty()) {
            return new CoreCounts(-1, -1, -1);
        }
        return new CoreCounts(siblings.size(), logical, logical / siblings.size());
    }

    /**
     * Counts NUMA nodes from the kernel node list.
     *
     * <p>Tries {@code possible} first and {@code online} second. Distances are not read: this manager
     * never schedules per node, so a distance table would be a promise it cannot keep.
     *
     * @param nodeRoot sysfs NUMA directory
     * @return node count, or {@code -1} when nothing could be read
     */
    private static int countNumaNodes(final Path nodeRoot) {
        final int possible = countCpuList(nodeRoot.resolve("possible"));
        if (possible > 0) {
            return possible;
        }
        return countCpuList(nodeRoot.resolve("online"));
    }

    /**
     * Reads the cgroup CPU quota in core equivalents.
     *
     * <p>Handles cgroup v2 ({@code cpu.max}) and v1 ({@code cpu.cfs_quota_us} and
     * {@code cpu.cfs_period_us}). A fractional quota floors to one core because a process always gets at
     * least one.
     *
     * @param cgroupRoot cgroup mount root
     * @return core equivalents, or {@code -1} when no quota applies
     */
    private static int readCgroupQuota(final Path cgroupRoot) {
        final String v2 = readTrimmed(cgroupRoot.resolve("cpu.max"));
        if (!v2.isEmpty()) {
            return parseCpuMax(v2);
        }
        final Path v1 = cgroupRoot.resolve("cpu");
        final String quota = readTrimmed(v1.resolve("cpu.cfs_quota_us"));
        final String period = readTrimmed(v1.resolve("cpu.cfs_period_us"));
        if (quota.isEmpty() || period.isEmpty()) {
            return -1;
        }
        return parseQuota(quota, period);
    }

    /**
     * Reads the effective cpuset size.
     *
     * <p>cgroup v2 exposes the resolved mask as {@code cpuset.cpus.effective}; v1 uses
     * {@code cpuset/cpuset.effective_cpus}. This is diagnostics: the JVM count already includes it.
     *
     * @param cgroupRoot cgroup mount root
     * @return CPU count, or {@code -1} when no cpuset applies
     */
    private static int readCgroupCpuset(final Path cgroupRoot) {
        final int v2 = countCpuList(cgroupRoot.resolve("cpuset.cpus.effective"));
        if (v2 > 0) {
            return v2;
        }
        final Path v1 = cgroupRoot.resolve("cpuset");
        final int effective = countCpuList(v1.resolve("cpuset.effective_cpus"));
        if (effective > 0) {
            return effective;
        }
        return countCpuList(v1.resolve("cpuset.cpus"));
    }

    /**
     * Parses a cgroup v2 quota line.
     *
     * @param text file content such as {@code 400000 100000} or {@code max 100000}
     * @return core equivalents, or {@code -1} for an unlimited or malformed quota
     */
    static int parseCpuMax(final String text) {
        final String[] parts = text.trim().split("\\s+");
        if (parts.length < 2 || NO_QUOTA.equals(parts[0])) {
            return -1;
        }
        return parseQuota(parts[0], parts[1]);
    }

    /**
     * Turns a quota and a period into core equivalents.
     *
     * @param quota  quota in microseconds
     * @param period period in microseconds
     * @return at least one core, or {@code -1} when the pair is unusable
     */
    static int parseQuota(final String quota, final String period) {
        try {
            final long quotaMicros = Long.parseLong(quota.trim());
            final long periodMicros = Long.parseLong(period.trim());
            if (quotaMicros <= 0L || periodMicros <= 0L) {
                return -1;
            }
            return (int) Math.max(1L, quotaMicros / periodMicros);
        } catch (NumberFormatException _) {
            return -1;
        }
    }

    /**
     * Counts CPUs in a cpulist file.
     *
     * @param file file holding text such as {@code 0-3,8-11}
     * @return the count, or {@code -1} when the file is missing or malformed
     */
    static int countCpuList(final Path file) {
        final String text = readTrimmed(file);
        if (text.isEmpty()) {
            return -1;
        }
        return parseCpuList(text);
    }

    /**
     * Parses a cpulist such as {@code 0-3,8-11}.
     *
     * @param text cpulist text
     * @return the number of CPUs it names, or {@code -1} when it is malformed
     */
    static int parseCpuList(final String text) {
        final String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return -1;
        }
        int total = 0;
        for (final String part : trimmed.split(",")) {
            final int dash = part.indexOf('-');
            try {
                if (dash < 0) {
                    final int cpu = Integer.parseInt(part.trim());
                    if (cpu < 0) {
                        return -1;
                    }
                    total++;
                } else {
                    final int first = Integer.parseInt(part.substring(0, dash).trim());
                    final int last = Integer.parseInt(part.substring(dash + 1).trim());
                    if (first < 0 || last < first) {
                        return -1;
                    }
                    total += last - first + 1;
                }
            } catch (NumberFormatException _) {
                return -1;
            }
        }
        return total;
    }

    /**
     * Reads a small text file and trims it.
     *
     * @param file file to read
     * @return the trimmed content, or an empty string when the file is absent or unreadable
     */
    private static String readTrimmed(final Path file) {
        if (!Files.isRegularFile(file)) {
            return "";
        }
        try {
            return Files.readString(file, StandardCharsets.US_ASCII).trim();
        } catch (IOException _) {
            return "";
        }
    }

    /**
     * Physical and logical core counts with the sibling ratio.
     *
     * @param physical        physical cores
     * @param logical         logical CPUs
     * @param siblingsPerCore logical CPUs per core
     */
    private record CoreCounts(int physical, int logical, int siblingsPerCore) {
    }
}
