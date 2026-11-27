package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Frozen description of the machine and the checkout a run belongs to.
 *
 * <p>The manifest is the only thing that makes two runs comparable later: if the git head, the patch
 * set, the JDK, the JVM flags, the CPU or the config hashes differ, the analyzer must be able to see
 * it without trusting a human written note. Everything here is read from the running process or from
 * git, never typed in by hand.
 */
final class HostEnvironment {

    private HostEnvironment() {
    }

    /**
     * Returns the files whose content identifies the applied patch set.
     *
     * <p>Only the repositories this project owns are listed; the generated upstream sources are not
     * part of the identity because they are reproducible from these patches.
     *
     * @param repo repository root
     * @return sorted patch files that exist
     */
    static List<Path> patchFiles(final Path repo) {
        final List<Path> roots = List.of(
            repo.resolve("tuff-server").resolve("paper-patches"),
            repo.resolve("tuff-server").resolve("folia-patches"),
            repo.resolve("tuff-server").resolve("minecraft-patches"),
            repo.resolve("tuff-api").resolve("paper-patches"),
            repo.resolve("tuff-api").resolve("folia-patches"),
            repo.resolve("tuff-checkstyle").resolve("config-patches"),
            repo.resolve("tuff-checkstyle").resolve("paper-patches")
        );
        final List<Path> files = new ArrayList<>();
        for (final Path root : roots) {
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> walk = Files.walk(root)) {
                walk.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".patch"))
                    .forEach(files::add);
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot list patches under " + root, failure);
            }
        }
        files.sort(Comparator.comparing(path -> repo.relativize(path).toString()));
        return List.copyOf(files);
    }

    /**
     * Hashes the patch set: relative path plus content of every patch file.
     *
     * @param repo repository root
     * @return {@code sha256:<hex>} and the number of files
     */
    static PatchSet patchSet(final Path repo) {
        final MessageDigest digest = sha256();
        final List<Path> files = patchFiles(repo);
        for (final Path file : files) {
            digest.update(repo.relativize(file).toString().replace('\\', '/').getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(readBytes(file));
            digest.update((byte) 0);
        }
        return new PatchSet(HexFormat.of().formatHex(digest.digest()), files.size());
    }

    /**
     * Hash of one file.
     *
     * @param file source
     * @return lowercase hex digest
     */
    static String sha256Hex(final Path file) {
        return HexFormat.of().formatHex(sha256().digest(readBytes(file)));
    }

    /**
     * Hashes the configuration files a server process actually read.
     *
     * @param runDir run directory
     * @return ordered map of relative path to digest, plus one combined digest under {@code combined}
     */
    static Map<String, String> configHashes(final Path runDir) {
        final List<String> names = List.of(
            "server.properties",
            "bukkit.yml",
            "spigot.yml",
            "config/paper-global.yml",
            "config/paper-world-defaults.yml",
            "config/tuff.yml"
        );
        final Map<String, String> hashes = new LinkedHashMap<>();
        final MessageDigest combined = sha256();
        for (final String name : names) {
            final Path file = runDir.resolve(name);
            if (!Files.isRegularFile(file)) {
                continue;
            }
            final String digest = sha256Hex(file);
            hashes.put(name, digest);
            combined.update(name.getBytes(StandardCharsets.UTF_8));
            combined.update((byte) 0);
            combined.update(digest.getBytes(StandardCharsets.UTF_8));
            combined.update((byte) 0);
        }
        hashes.put("combined", HexFormat.of().formatHex(combined.digest()));
        return hashes;
    }

    /**
     * Reads git head, branch and dirty state.
     *
     * @param repo repository root
     * @return map with {@code head}, {@code branch}, {@code dirty} and {@code changed_files}
     */
    static Map<String, Object> git(final Path repo) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("head", git(repo, "rev-parse", "HEAD"));
        map.put("branch", git(repo, "rev-parse", "--abbrev-ref", "HEAD"));
        final String status = git(repo, "status", "--porcelain");
        final List<String> changed = status == null || status.isBlank()
            ? List.of()
            : status.lines().filter(line -> !line.isBlank()).toList();
        map.put("dirty", !changed.isEmpty());
        map.put("changed_files", changed);
        return map;
    }

    /**
     * Describes the host: CPU, memory and OS.
     *
     * @return map written into the manifest
     */
    static Map<String, Object> host() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("cpu_model", cpuModel());
        map.put("cores_logical", Runtime.getRuntime().availableProcessors());
        map.put("ram_bytes", totalMemory());
        map.put("os_name", System.getProperty("os.name", "unknown"));
        map.put("os_version", System.getProperty("os.version", "unknown"));
        map.put("os_arch", System.getProperty("os.arch", "unknown"));
        map.put("kernel", kernel());
        map.put("hw_tag", hwTag());
        return map;
    }

    /**
     * Describes the JVM that runs the harness and, by construction, the server child process.
     *
     * @return map written into the manifest
     */
    static Map<String, Object> jdk() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("version", System.getProperty("java.version", "unknown"));
        map.put("vendor", System.getProperty("java.vendor", "unknown"));
        map.put("vm", System.getProperty("java.vm.name", "unknown") + ' ' + System.getProperty("java.vm.version", ""));
        map.put("runtime", Runtime.version().toString());
        map.put("java_home", System.getProperty("java.home", "unknown"));
        return map;
    }

    /**
     * Returns a short host tag used in the block identifier.
     *
     * @return e.g. {@code win16} or {@code linux8}
     */
    static String hwTag() {
        final String os = System.getProperty("os.name", "unknown").toLowerCase(Locale.ROOT);
        final String family = os.contains("win") ? "win" : os.contains("mac") ? "mac" : "linux";
        return family + Runtime.getRuntime().availableProcessors();
    }

    private static String cpuModel() {
        final String fromEnvironment = System.getenv("PROCESSOR_IDENTIFIER");
        if (fromEnvironment != null && !fromEnvironment.isBlank()) {
            return fromEnvironment.strip();
        }
        final Path cpuinfo = Path.of("/proc/cpuinfo");
        if (Files.isReadable(cpuinfo)) {
            try {
                for (final String line : Files.readAllLines(cpuinfo, StandardCharsets.UTF_8)) {
                    if (line.startsWith("model name")) {
                        final int colon = line.indexOf(':');
                        if (colon > 0) {
                            return line.substring(colon + 1).strip();
                        }
                    }
                }
            } catch (final IOException failure) {
                return "unreadable /proc/cpuinfo: " + failure.getMessage();
            }
        }
        return System.getProperty("os.arch", "unknown");
    }

    private static String kernel() {
        final Path release = Path.of("/proc/sys/kernel/osrelease");
        if (Files.isReadable(release)) {
            try {
                return Files.readString(release, StandardCharsets.UTF_8).strip();
            } catch (final IOException failure) {
                return "unreadable: " + failure.getMessage();
            }
        }
        return "n/a on this platform";
    }

    private static long totalMemory() {
        try {
            final var bean = ManagementFactory.getOperatingSystemMXBean();
            if (bean instanceof final com.sun.management.OperatingSystemMXBean extended) {
                return extended.getTotalMemorySize();
            }
        } catch (final RuntimeException failure) {
            return -1L;
        }
        return -1L;
    }

    private static String git(final Path repo, final String... arguments) {
        final List<String> command = new ArrayList<>();
        command.add("git");
        command.add("-C");
        command.add(repo.toString());
        command.addAll(List.of(arguments));
        try {
            final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(20, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return null;
            }
            return process.exitValue() == 0 ? output.strip() : null;
        } catch (final IOException failure) {
            return null;
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static byte[] readBytes(final Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot read " + file, failure);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is mandated by the JDK", failure);
        }
    }

    /**
     * Identity of the applied patch set.
     *
     * @param digest {@code sha256:<hex>}
     * @param files  number of patch files hashed
     */
    record PatchSet(String digest, int files) {
    }
}
