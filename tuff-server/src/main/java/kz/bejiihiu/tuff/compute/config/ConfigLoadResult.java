package kz.bejiihiu.tuff.compute.config;

import java.util.List;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.ComputeSettings;

/**
 * Outcome of reading {@code config/tuff.yml}.
 *
 * <p>A result is always usable: either it carries freshly parsed settings or, when the file could not
 * be read or parsed at all, the snapshot the scheduler was already running plus a problem explaining
 * why. {@link #keptPreviousSettings()} tells the two apart.
 *
 * @param settings              settings that are now in effect, never {@code null}
 * @param problems              everything that was ignored, replaced by a default or could not be
 *                              applied, in file order
 * @param diff                  what changed compared to the previously published snapshot; empty on
 *                              the first read and for an untouched file
 * @param createdFile           whether the file was missing and has just been written
 * @param keptPreviousSettings  whether reading failed and the previous snapshot stays in effect
 */
public record ConfigLoadResult(
    ComputeSettings settings,
    List<ConfigProblem> problems,
    ConfigDiff diff,
    boolean createdFile,
    boolean keptPreviousSettings
) {

    /**
     * Validates and freezes the result.
     *
     * @throws NullPointerException if a component is {@code null}
     */
    public ConfigLoadResult {
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(problems, "problems");
        Objects.requireNonNull(diff, "diff");
        problems = List.copyOf(problems);
    }

    /**
     * Checks whether anything went wrong or was ignored.
     *
     * @return {@code true} if {@link #problems()} is not empty
     */
    public boolean hasProblems() {
        return !this.problems.isEmpty();
    }
}
