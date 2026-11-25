package kz.bejiihiu.tuff.compute.config;

import java.util.Objects;

/**
 * A single problem found while reading {@code config/tuff.yml}.
 *
 * <p>Problems are advisory: the loader always keeps a usable {@code ComputeSettings} tree, so a
 * problem describes what was ignored, replaced by a default or could not be applied rather than a
 * reason to stop the server.
 *
 * @param key    location of the problem, either a dotted key path such as
 *               {@code async-compute.workers.threads} or a run description such as
 *               {@code config/tuff.yml}
 * @param detail what is wrong and what the loader did instead, in plain words for an administrator
 */
public record ConfigProblem(String key, String detail) {

    /**
     * Validates the problem.
     *
     * @throws NullPointerException if a component is {@code null}
     */
    public ConfigProblem {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(detail, "detail");
    }

    /**
     * Returns the problem in the form used by log lines and command output.
     *
     * @return {@code key: detail}, or the bare detail when the key is empty
     */
    @Override
    public String toString() {
        if (this.key.isEmpty()) {
            return this.detail;
        }
        return this.key + ": " + this.detail;
    }
}
