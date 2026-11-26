package kz.bejiihiu.tuff.threads.internal.guard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fails when Tuff owned sources create threads or pools by hand outside the allowlist.
 *
 * <p>The registry knows a pool only if somebody declared it, and the factory knows a thread only if it
 * built it. A stray {@code new Thread}, a private {@code ForkJoinPool} or a default {@code Executors}
 * pool is exactly the leak UTMI exists to close, so it is cheaper to fail the build than to find it in
 * a thread dump a year later.
 *
 * <p>What is scanned: {@code src/main/java} of {@code tuff-compute}, {@code tuff-threads} and
 * {@code tuff-api}, plus our own package inside {@code tuff-server}. Tests and benchmarks are out of
 * scope on purpose - a test may spin whatever it wants to make a case.
 *
 * <p>Comments and string literals are masked before matching; the report carries {@code file:line} and
 * a one line hint. The allowlist lives in {@link #allowlist()} with a reason per entry, and an entry
 * that no longer matches anything fails as stale: an exception that outlived its cause is how guards
 * quietly turn into decoration.
 */
class RawThreadCreationGuardTest {

    private static final int MAX_PARENT_LOOKUPS = 6;

    private static final List<Forbidden> FORBIDDEN = List.of(
        new Forbidden(
            "new Thread(",
            "build threads through ThreadFactoryProvider, or register and migrate the pool first"
        ),
        new Forbidden(
            "Thread.ofPlatform(",
            "platform threads are created by the thread factory, not by hand"
        ),
        new Forbidden(
            "Thread.ofVirtual(",
            "a virtual lane is created by its owner and wrapped for accounting, not opened ad hoc"
        ),
        new Forbidden(
            "Executors.",
            "an executor is a hidden pool: register a PoolSpec and let the manager own the factory"
        ),
        new Forbidden(
            "new ThreadPoolExecutor",
            "a hand built pool has no spec, no budget and no metrics; use a registered pool"
        ),
        new Forbidden(
            "new ForkJoinPool",
            "a private fork join pool hides its threads from the registry"
        ),
        new Forbidden(
            "new ScheduledThreadPoolExecutor",
            "use the registered service pool or the manager timer seam"
        )
    );

    private static final List<ScanRoot> ROOTS = List.of(
        new ScanRoot("tuff-compute/src/main/java"),
        new ScanRoot("tuff-threads/src/main/java"),
        new ScanRoot("tuff-api/src/main/java"),
        new ScanRoot("tuff-server/src/main/java/kz/bejiihiu/tuff")
    );

    private static final Map<String, String> ALLOWLIST = allowlist();

    @Test
    void noRawThreadsOutsideTheAllowlist() throws IOException {
        final Path root = repoRoot();
        final List<String> violations = new ArrayList<>();
        final Set<String> matchedAllowlistEntries = new HashSet<>();

        for (final ScanRoot scanRoot : ROOTS) {
            final Path directory = root.resolve(scanRoot.path());
            assertTrue(Files.isDirectory(directory), "scan root does not exist: " + directory);
            try (Stream<Path> files = Files.walk(directory)) {
                for (final Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                    final String relative = root.relativize(file).toString().replace('\\', '/');
                    final List<String> found = violationsIn(file, relative);
                    if (found.isEmpty()) {
                        continue;
                    }
                    if (ALLOWLIST.containsKey(relative)) {
                        matchedAllowlistEntries.add(relative);
                    } else {
                        violations.addAll(found);
                    }
                }
            }
        }

        for (final Map.Entry<String, String> entry : ALLOWLIST.entrySet()) {
            if (!matchedAllowlistEntries.contains(entry.getKey())) {
                violations.add(
                    "allowlist: " + entry.getKey() + " no longer needs an exception ("
                        + entry.getValue() + ") - remove the entry or restore the call"
                );
            }
        }

        assertTrue(
            violations.isEmpty(),
            () -> "raw thread creation is only legal inside the allowlist:\n"
                + String.join("\n", violations)
        );
    }

    /**
     * The files that may still build threads themselves, each with the reason it exists.
     *
     * <p>Every entry is temporary except the provider: compute is rewired through UTMI in phase 3 and
     * the server side JFR service follows its sink.
     *
     * @return repo relative path to the reason
     */
    private static Map<String, String> allowlist() {
        final Map<String, String> allowlist = new LinkedHashMap<>();
        // the one legal place a managed platform thread is constructed; everything else goes through it
        allowlist.put(
            "tuff-threads/src/main/java/kz/bejiihiu/tuff/threads/internal/impl/factory/"
                + "ThreadFactoryProviderImpl.java",
            "the provider itself, this is where managed threads are born"
        );
        // compute predates utmi; its factory is the single creation point until phase 3 rewires the engine
        allowlist.put(
            "tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/internal/util/ThreadFactories.java",
            "compute's own factory, the legacy single creation point"
        );
        // legacy compute engine: completion executor, janitor, timer and workers live here today
        allowlist.put(
            "tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/internal/engine/impl/ComputeEngineImpl.java",
            "legacy compute engine internals, replaced by the factory in phase 3"
        );
        // compute sampler executor is built on ThreadFactories.platform, i.e. the factory is centralised
        allowlist.put(
            "tuff-compute/src/main/java/kz/bejiihiu/tuff/compute/internal/metrics/impl/"
                + "ComputeMetricsCollector.java",
            "compute sampler executor built on ThreadFactories, adopted with the rest of compute"
        );
        // server side JFR helper predates the utmi JFR sink; its executor uses a named platform factory
        allowlist.put(
            "tuff-server/src/main/java/kz/bejiihiu/tuff/compute/bootstrap/ComputeJfr.java",
            "server compute JFR helper, moves to the utmi event sink later"
        );
        return Map.copyOf(allowlist);
    }

    private static List<String> violationsIn(final Path file, final String relative) throws IOException {
        final List<String> code = maskCommentsAndLiterals(Files.readAllLines(file, StandardCharsets.UTF_8));
        final List<String> found = new ArrayList<>();
        for (int index = 0; index < code.size(); index++) {
            final String line = code.get(index);
            for (final Forbidden forbidden : FORBIDDEN) {
                if (line.contains(forbidden.token())) {
                    found.add(
                        relative + ":" + (index + 1) + ": " + forbidden.token() + " - " + forbidden.advice()
                    );
                }
            }
        }
        return found;
    }

    /**
     * Replaces comments, string literals and char literals with spaces, keeping the line count.
     *
     * <p>Good enough for a guard: it must not fire on a javadoc example, and it must not silently ignore
     * a call just because a URL earlier on the line looks like a comment. Escape sequences are skipped so
     * a quote inside a literal does not end it early.
     *
     * @param lines source lines
     * @return the same lines with non code characters blanked
     */
    private static List<String> maskCommentsAndLiterals(final List<String> lines) {
        final List<String> masked = new ArrayList<>(lines.size());
        boolean blockComment = false;
        boolean textBlock = false;
        for (final String line : lines) {
            final StringBuilder out = new StringBuilder(line.length());
            boolean string = false;
            boolean character = false;
            int index = 0;
            while (index < line.length()) {
                final char current = line.charAt(index);
                final char next = index + 1 < line.length() ? line.charAt(index + 1) : '\0';
                if (blockComment) {
                    if (current == '*' && next == '/') {
                        blockComment = false;
                        index += 2;
                    } else {
                        index++;
                    }
                    out.append(' ');
                    continue;
                }
                if (textBlock) {
                    if (line.startsWith("\"\"\"", index)) {
                        textBlock = false;
                        index += 3;
                    } else {
                        index++;
                    }
                    out.append(' ');
                    continue;
                }
                if (string || character) {
                    final char closing = string ? '"' : '\'';
                    if (current == '\\') {
                        index += 2;
                    } else {
                        if (current == closing) {
                            string = false;
                            character = false;
                        }
                        index++;
                    }
                    out.append(' ');
                    continue;
                }
                if (current == '/' && next == '/') {
                    break;
                }
                if (current == '/' && next == '*') {
                    blockComment = true;
                    index += 2;
                    out.append(' ');
                    continue;
                }
                if (line.startsWith("\"\"\"", index)) {
                    textBlock = true;
                    index += 3;
                    out.append(' ');
                    continue;
                }
                if (current == '"') {
                    string = true;
                    index++;
                    out.append(' ');
                    continue;
                }
                if (current == '\'') {
                    character = true;
                    index++;
                    out.append(' ');
                    continue;
                }
                out.append(current);
                index++;
            }
            masked.add(out.toString());
        }
        return masked;
    }

    private static Path repoRoot() {
        final Path start = Paths.get("").toAbsolutePath();
        Path candidate = start;
        for (int lookup = 0; lookup < MAX_PARENT_LOOKUPS && candidate != null; lookup++) {
            if (Files.isRegularFile(candidate.resolve("settings.gradle.kts"))
                && Files.isDirectory(candidate.resolve("tuff-threads"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("cannot find the repository root from " + start);
    }

    private record Forbidden(String token, String advice) {
    }

    private record ScanRoot(String path) {
    }
}
