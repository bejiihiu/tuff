package kz.bejiihiu.tuff.compute.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Differences between two {@code ComputeSettings} snapshots, expressed as dotted keys of
 * {@code config/tuff.yml}.
 *
 * <p>The diff exists for two consumers: the reload log and the {@code /tuff} command report what
 * changed, and the engine apply hook gets a pre-filtered list of changes that cannot be applied to a
 * live pool. Keys in {@link #changedPaths()} that are not in {@link #restartRequiredPaths()} are the
 * ones an engine implementation may pick up at runtime.
 *
 * <p>The engine takes its settings as one immutable snapshot at start, so most of the tree is
 * startup-only: worker sizing, queue capacity, scheduling, metrics, adaptive control, the lanes and
 * the shutdown grace period all need a restart even though the file keeps the new value for the next
 * start. Only consumer policies (and {@code queue.overflow-policy}, which feeds them) can change on
 * a live pool. The whole {@code thread-management} section is startup-only as well.
 *
 * @param changedPaths         every key whose effective value differs, sorted
 * @param restartRequiredPaths the subset that a running scheduler cannot adopt safely, sorted
 */
public record ConfigDiff(List<String> changedPaths, List<String> restartRequiredPaths) {

    /**
     * Keys that are baked into the engine at start and need a restart. Prefix entries end with a
     * dot and match every key below them.
     */
    private static final Set<String> RESTART_EXACT = Set.of(
        "async-compute.enabled",
        "async-compute.queue.capacity",
        "async-compute.consumers.allow-dynamic-registration",
        "async-compute.shutdown-grace-seconds"
    );

    private static final List<String> RESTART_PREFIXES = List.of(
        "async-compute.workers.",
        "async-compute.scheduling.",
        "async-compute.metrics.",
        "async-compute.adaptive.",
        "async-compute.lanes.",
        // the thread layer reads its whole section once, when it starts: no key of it is live
        "thread-management."
    );

    /**
     * Validates and freezes the diff.
     *
     * @throws NullPointerException if a component is {@code null}
     */
    public ConfigDiff {
        changedPaths = List.copyOf(changedPaths);
        restartRequiredPaths = List.copyOf(restartRequiredPaths);
    }

    /**
     * Returns the diff of a fresh load against the shipped defaults, which is empty for an untouched
     * file.
     *
     * @return an empty diff
     */
    public static ConfigDiff empty() {
        return new ConfigDiff(List.of(), List.of());
    }

    /**
     * Compares two flattened snapshots.
     *
     * @param before flattened settings before the change
     * @param after  flattened settings after the change
     * @return the diff between both snapshots
     */
    static ConfigDiff between(final Map<String, String> before, final Map<String, String> after) {
        final Set<String> keys = new TreeSet<>(before.keySet());
        keys.addAll(after.keySet());

        final List<String> changed = new ArrayList<>();
        final List<String> restart = new ArrayList<>();
        for (final String key : keys) {
            if (Objects.equals(before.get(key), after.get(key))) {
                continue;
            }
            changed.add(key);
            if (needsRestart(key)) {
                restart.add(key);
            }
        }
        return new ConfigDiff(changed, restart);
    }

    private static boolean needsRestart(final String path) {
        if (RESTART_EXACT.contains(path)) {
            return true;
        }
        for (final String prefix : RESTART_PREFIXES) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the two snapshots are identical.
     *
     * @return {@code true} if nothing changed
     */
    public boolean isEmpty() {
        return this.changedPaths.isEmpty();
    }

    /**
     * Returns the changed keys an engine may adopt without a restart.
     *
     * @return changed keys minus the restart-only ones, sorted
     */
    public List<String> livePaths() {
        final List<String> live = new ArrayList<>(this.changedPaths);
        live.removeAll(this.restartRequiredPaths);
        return List.copyOf(live);
    }

    /**
     * Returns a comma separated list of changed keys for a log line.
     *
     * @return the changed keys, or an empty string when nothing changed
     */
    public String summary() {
        return String.join(", ", this.changedPaths);
    }

    /**
     * Returns a comma separated list of the keys that need a restart.
     *
     * @return the restart-only keys, or an empty string when there are none
     */
    public String restartSummary() {
        return String.join(", ", this.restartRequiredPaths);
    }
}
