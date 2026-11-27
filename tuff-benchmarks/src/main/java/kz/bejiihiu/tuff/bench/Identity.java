package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The fixed identity of a block of runs.
 *
 * <p>Every run in one block shares the checkout, the jar, the JDK, the host and therefore these
 * values. The analyzer refuses to pool runs whose identity differs, because two different jars are
 * two different experiments even when the arm label matches.
 *
 * @param benchId   block identifier, {@code <date>_<profile>_<git7>_<hwtag>}
 * @param gitHead   git commit of the checkout
 * @param gitBranch branch name, or the empty string
 * @param gitDirty  whether the checkout had uncommitted changes
 * @param changedFiles files that were dirty, for the record
 * @param patchDigest digest of the applied patch set
 * @param patchFiles  number of patch files hashed
 * @param jarPath   absolute jar path
 * @param jarSha    digest of the jar
 * @param jdk       short JDK description
 * @param javaHome  java home that runs both the harness and the server
 * @param hostTag   host identifier
 * @param host      host facts
 * @param jdkFacts  JVM facts
 */
record Identity(
    String benchId,
    String gitHead,
    String gitBranch,
    boolean gitDirty,
    List<String> changedFiles,
    String patchDigest,
    int patchFiles,
    String jarPath,
    String jarSha,
    String jdk,
    String javaHome,
    String hostTag,
    Map<String, Object> host,
    Map<String, Object> jdkFacts
) {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withZone(ZoneOffset.UTC);

    /**
     * Freezes the file list and maps.
     */
    Identity {
        changedFiles = List.copyOf(changedFiles);
        host = Map.copyOf(host);
        jdkFacts = Map.copyOf(jdkFacts);
    }

    /**
     * Builds the identity of a block from the checkout, the jar and the current process.
     *
     * @param repoRoot repository root
     * @param jar      server jar
     * @param profile  profile, part of the block identifier
     * @param benchId  explicit block identifier, or {@code null} to derive one
     * @return the identity
     * @throws IOException if the jar cannot be read
     */
    static Identity probe(final Path repoRoot, final Path jar, final LoadProfile profile, final String benchId)
        throws IOException {
        final Path absoluteJar = jar.toAbsolutePath().normalize();
        if (!Files.isRegularFile(absoluteJar)) {
            throw new IOException("server jar not found: " + absoluteJar);
        }
        final Map<String, Object> git = HostEnvironment.git(repoRoot);
        final HostEnvironment.PatchSet patches = HostEnvironment.patchSet(repoRoot);
        final Map<String, Object> host = HostEnvironment.host();
        final Map<String, Object> jdkFacts = HostEnvironment.jdk();
        final String javaHome = System.getProperty("java.home", "");
        final String jdk = jdkFacts.get("vendor") + " " + jdkFacts.get("version");
        final String hostTag = String.valueOf(host.get("hw_tag"));
        final String head = String.valueOf(git.getOrDefault("head", "unknown"));
        final String resolvedBenchId = benchId == null || benchId.isBlank()
            ? DATE.format(Instant.now()) + '_' + profile.cli() + '_' + shortHead(head) + '_' + hostTag
            : benchId;
        final List<String> changed = git.get("changed_files") instanceof final List<?> list
            ? list.stream().map(String::valueOf).toList()
            : List.of();
        return new Identity(
            resolvedBenchId,
            head,
            String.valueOf(git.getOrDefault("branch", "")),
            Boolean.TRUE.equals(git.get("dirty")),
            changed,
            patches.digest(),
            patches.files(),
            absoluteJar.toString(),
            HostEnvironment.sha256Hex(absoluteJar),
            jdk,
            javaHome,
            hostTag,
            host,
            jdkFacts
        );
    }

    private static String shortHead(final String head) {
        return head.length() >= 7 ? head.substring(0, 7) : head;
    }

    /**
     * Returns the block record written next to the runs.
     *
     * @param options the options of the block
     * @return ordered map with the environment and the planned windows
     */
    Map<String, Object> toMap(final BenchOptions options) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("bench_id", this.benchId);
        map.put("git_head", this.gitHead);
        map.put("git_branch", this.gitBranch);
        map.put("git_dirty", this.gitDirty);
        map.put("git_changed_files", List.copyOf(this.changedFiles));
        map.put("patchset_hash", this.patchDigest);
        map.put("patchset_files", this.patchFiles);
        map.put("jar_path", this.jarPath);
        map.put("jar_sha256", this.jarSha);
        map.put("jdk", this.jdk);
        map.put("java_home", this.javaHome);
        map.put("host_tag", this.hostTag);
        map.put("host", this.host);
        map.put("jvm", this.jdkFacts);
        map.put("profile", options.profile.cli());
        map.put("threads", options.threads);
        map.put("max_threads", options.maxThreads);
        map.put("warmup_s", options.warmupSeconds);
        map.put("measure_s", options.measureSeconds);
        map.put("sample_ms", options.sampleMillis);
        map.put("load", Map.of(
            "areas", options.areas,
            "area_chunks", options.areaChunks,
            "area_step", options.areaStep,
            "entities_per_area", options.entitiesPerArea,
            "entity_type", options.entityType
        ));
        return map;
    }
}
