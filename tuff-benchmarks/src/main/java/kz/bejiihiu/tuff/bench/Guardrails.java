package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Hard failures that invalidate a run regardless of how good its numbers look.
 *
 * <p>The methodology is explicit that a fast run with a watchdog dump is not evidence. The checks are
 * deliberately few and named: a watchdog dump, an asynchronous-catcher violation, a failed region tick,
 * an out-of-memory error and a non-zero exit code. Every match keeps the exact line, so the report can
 * quote the evidence instead of claiming a class of failure.
 *
 * <p>Warnings that are part of normal operation (missing config keys, plugin messages, the JFR
 * deprecation notice) are not in the list; treating them as failures would make every run invalid.
 */
final class Guardrails {

    private static final Map<String, List<String>> PATTERNS = patterns();

    private Guardrails() {
    }

    private static Map<String, List<String>> patterns() {
        final Map<String, List<String>> map = new LinkedHashMap<>();
        map.put("watchdog_dump", List.of("foliawatchdogthread", "has not responded in", "server has stopped responding"));
        map.put("async_catcher", List.of(
            "asynchronous catcher", "async catcher", "asynccatcher",
            "asynchronous task", "asynchronous region task"
        ));
        map.put("region_failure", List.of("region failed", "failed to tick region", "exception ticking"));
        map.put("out_of_memory", List.of("outofmemoryerror"));
        map.put("uncaught_exception", List.of(
            "encountered an unexpected exception", "exception in thread", "uncaught exception"
        ));
        return Map.copyOf(map);
    }

    /**
     * Inspects the captured log and the process exit code.
     *
     * @param logPath  captured console log
     * @param exitCode process exit code
     * @return the reasons found and one evidence line per reason
     * @throws IOException if the log cannot be read
     */
    static Result inspect(final Path logPath, final int exitCode) throws IOException {
        final Map<String, String> evidence = new LinkedHashMap<>();
        if (Files.isRegularFile(logPath)) {
            for (final String line : Files.readAllLines(logPath, StandardCharsets.UTF_8)) {
                final String lower = line.toLowerCase(Locale.ROOT);
                for (final Map.Entry<String, List<String>> entry : PATTERNS.entrySet()) {
                    if (evidence.containsKey(entry.getKey())) {
                        continue;
                    }
                    for (final String needle : entry.getValue()) {
                        if (lower.contains(needle)) {
                            evidence.put(entry.getKey(), trim(line));
                            break;
                        }
                    }
                }
            }
        }
        final List<String> reasons = new ArrayList<>(evidence.keySet());
        if (exitCode != 0) {
            reasons.add("exit_code=" + exitCode);
            evidence.put("exit_code", "the server process left with " + exitCode);
        }
        return new Result(List.copyOf(reasons), Map.copyOf(evidence));
    }

    private static String trim(final String line) {
        final String stripped = line.strip();
        return stripped.length() <= 400 ? stripped : stripped.substring(0, 400) + "...";
    }

    /**
     * Guardrail findings.
     *
     * @param reasons  reason codes, empty when the run passed
     * @param evidence first matching line per reason
     */
    record Result(List<String> reasons, Map<String, String> evidence) {

        /**
         * Returns whether the run passed every guardrail.
         *
         * @return {@code true} when no reason was found
         */
        boolean clean() {
            return this.reasons.isEmpty();
        }
    }
}
