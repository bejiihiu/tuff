package kz.bejiihiu.tuff.threads;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Stable identifier of an execution pool.
 *
 * <p>The value is what registry lookups, snapshots, JFR events and commands show, so it is part of the
 * observable surface: renaming a pool id breaks dashboards and log parsers. Ids are lower case,
 * between one and sixty four characters, start with a letter or digit and may contain letters,
 * digits, dots, underscores and dashes.
 *
 * <p>This type is an immutable value: two ids with the same text are equal and hash to the same
 * bucket, and {@link #toString()} returns exactly the text so it can be dropped into a config key,
 * a log line or a command argument without quoting.
 *
 * @param value pool id text
 */
public record PoolId(String value) implements Comparable<PoolId> {

    /**
     * Canonical id of the fixed region tick pool.
     *
     * <p>The pool itself is owned by Folia; UTMI only observes it and, behind a flag, supplies its
     * thread factory. It is never shared with compute work.
     */
    public static final PoolId REGION_EXECUTION = new PoolId("region-execution");

    /**
     * Canonical id of the platform worker pool of the compute engine.
     */
    public static final PoolId COMPUTE = new PoolId("compute");

    /**
     * Canonical id of the virtual thread lane of the compute engine.
     *
     * <p>Virtual threads have no pool of their own in the JVM sense; the id exists so lease accounting
     * and snapshots can separate the platform lane from the virtual one.
     */
    public static final PoolId COMPUTE_VIRTUAL = new PoolId("compute-virtual");

    /**
     * Canonical id of the chunk generation and maintenance worker pool.
     */
    public static final PoolId CHUNK_WORKER = new PoolId("chunk-worker");

    /**
     * Canonical id of the chunk file IO worker pool.
     */
    public static final PoolId CHUNK_IO = new PoolId("chunk-io");

    /**
     * Canonical id of the network event loop pools.
     */
    public static final PoolId NETWORK = new PoolId("network");

    /**
     * Canonical id of the chat decoration executor.
     *
     * <p>Paper owns the {@code ThreadPoolExecutor}; UTMI only observes its size and its threads.
     */
    public static final PoolId CHAT = new PoolId("chat");

    /**
     * Canonical id of the generic background worker pool.
     *
     * <p>{@code Util.backgroundExecutor} is Paper's fork-join pool for anti-xray, structure placement
     * and other non-tick work. UTMI observes it and never touches its queue or sizing.
     */
    public static final PoolId BACKGROUND = new PoolId("background");

    /**
     * Canonical id of the catch all pool for short lived server services that do not deserve a pool
     * kind of their own.
     */
    public static final PoolId SERVICE = new PoolId("service");

    /**
     * Validates and creates an id.
     *
     * @param value pool id text
     * @throws NullPointerException     if {@code value} is {@code null}
     * @throws IllegalArgumentException if the text does not match the id format
     */
    public PoolId {
        Objects.requireNonNull(value, "value");
        if (!Format.PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                "pool id must match " + Format.PATTERN.pattern() + ", got '" + value + "'"
            );
        }
    }

    /**
     * Creates an id from text.
     *
     * @param value pool id text
     * @return the id
     * @throws NullPointerException     if {@code value} is {@code null}
     * @throws IllegalArgumentException if the text does not match the id format
     */
    public static PoolId of(final String value) {
        return new PoolId(value);
    }

    /**
     * Compares ids by their text, which gives commands and snapshots a deterministic order.
     *
     * @param other id to compare with
     * @return the usual comparison result of the two id strings
     * @throws NullPointerException if {@code other} is {@code null}
     */
    @Override
    public int compareTo(final PoolId other) {
        return this.value.compareTo(Objects.requireNonNull(other, "other").value);
    }

    /**
     * Returns the id text.
     *
     * @return the id exactly as it was supplied
     */
    @Override
    public String toString() {
        return this.value;
    }

    /**
     * Lazy holder for the id pattern.
     *
     * <p>Constants above are initialized before a plain private static field of the record would be, so
     * the pattern lives in its own class and is initialized on first use.
     */
    private static final class Format {

        private static final Pattern PATTERN = Pattern.compile("[a-z0-9][a-z0-9._-]{0,63}");

        private Format() {
        }
    }
}
