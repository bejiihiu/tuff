package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Reads the scheduler's own JFR events: one record per executed tick and one per supervisor pass.
 *
 * <p>This is the true per-tick source. {@code tuff.scheduler.Tick} carries the deadline, the start and
 * the end of the body, how many whole periods the handle was already behind and how many periods the
 * body wrote off, so a hard miss is counted instead of inferred from a five second TPS average.
 * {@code tuff.scheduler.Fund} carries the supervisor's view of the fund, which is what explains a hard
 * miss: no permit, or a slow body.
 *
 * <p>The recording is filtered with the JDK's own {@code jfr print --json}, parsed one event object at
 * a time so a long run does not need the whole document in memory. If the tool is missing, the
 * recording is unreadable or the events are absent, the result says so and the caller falls back to the
 * sampled aggregate.
 */
final class JfrTicks {

    /** Name of the per-tick event. */
    static final String TICK_EVENT = "tuff.scheduler.Tick";

    /** Name of the per-pass event. */
    static final String FUND_EVENT = "tuff.scheduler.Fund";

    /** Tick period of a 20 TPS region scheduler, in nanoseconds. */
    static final long TICK_PERIOD_NANOS = TimeUnit.MILLISECONDS.toNanos(50L);

    private JfrTicks() {
    }

    /**
     * Reads the per-tick and per-pass events inside a window.
     *
     * @param recording  recording file
     * @param jfrTool    path to the JDK's {@code jfr} executable
     * @param fromEpoch  window start, inclusive, epoch milliseconds
     * @param toEpoch    window end, exclusive, epoch milliseconds
     * @return the events, never {@code null}
     */
    static Result read(final Path recording, final Path jfrTool, final long fromEpoch, final long toEpoch) {
        if (!Files.isRegularFile(recording)) {
            return Result.absent("recording missing");
        }
        if (!Files.isRegularFile(jfrTool)) {
            return Result.absent("jfr tool missing: " + jfrTool);
        }
        final List<Tick> ticks = new ArrayList<>();
        final List<Fund> fund = new ArrayList<>();
        final Process process;
        try {
            process = new ProcessBuilder(
                jfrTool.toString(),
                "print",
                "--json",
                "--stack-depth",
                "0",
                "--events",
                TICK_EVENT + ',' + FUND_EVENT,
                recording.toString()
            ).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        } catch (final IOException failure) {
            return Result.absent("jfr tool failed to start: " + failure.getMessage());
        }
        try (Reader reader = process.inputReader(StandardCharsets.UTF_8)) {
            readEvents(reader, event -> {
                final String type = String.valueOf(event.get("type"));
                final Object values = event.get("values");
                if (!(values instanceof final Map<?, ?> map)) {
                    return;
                }
                if (TICK_EVENT.equals(type)) {
                    final Tick tick = tick(map);
                    if (tick != null && tick.epochMillis() >= fromEpoch && tick.epochMillis() < toEpoch) {
                        ticks.add(tick);
                    }
                } else if (FUND_EVENT.equals(type)) {
                    final Fund pass = fund(map);
                    if (pass != null && pass.epochMillis() >= fromEpoch && pass.epochMillis() < toEpoch) {
                        fund.add(pass);
                    }
                }
            });
        } catch (final IOException failure) {
            process.destroyForcibly();
            return Result.absent("recording unreadable: " + failure.getMessage());
        }
        try {
            if (!process.waitFor(3L, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                return Result.absent("jfr tool did not finish in time");
            }
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            return Result.absent("interrupted while reading the recording");
        }
        if (process.exitValue() != 0 && ticks.isEmpty() && fund.isEmpty()) {
            return Result.absent("jfr tool exited with " + process.exitValue());
        }
        return new Result(List.copyOf(ticks), List.copyOf(fund), "");
    }

    /**
     * Parses a {@code jfr print --json} document.
     *
     * <p>Package private so the harness self-check can exercise the parser with a synthetic document
     * instead of a whole recording.
     *
     * @param document JSON document
     * @param fromEpoch window start, inclusive
     * @param toEpoch window end, exclusive
     * @return the events, never {@code null}
     * @throws IOException if the document cannot be read
     */
    static Result parseJson(final String document, final long fromEpoch, final long toEpoch) throws IOException {
        final List<Tick> ticks = new ArrayList<>();
        final List<Fund> fund = new ArrayList<>();
        readEvents(new java.io.StringReader(document), event -> {
            final String type = String.valueOf(event.get("type"));
            final Object values = event.get("values");
            if (!(values instanceof final Map<?, ?> map)) {
                return;
            }
            if (TICK_EVENT.equals(type)) {
                final Tick tick = tick(map);
                if (tick != null && tick.epochMillis() >= fromEpoch && tick.epochMillis() < toEpoch) {
                    ticks.add(tick);
                }
            } else if (FUND_EVENT.equals(type)) {
                final Fund pass = fund(map);
                if (pass != null && pass.epochMillis() >= fromEpoch && pass.epochMillis() < toEpoch) {
                    fund.add(pass);
                }
            }
        });
        return new Result(List.copyOf(ticks), List.copyOf(fund), "");
    }

    private static Tick tick(final Map<?, ?> values) {
        final long epoch = epoch(values.get("startTime"));
        if (epoch < 0L) {
            return null;
        }
        return new Tick(
            integer(values, "regionId", -1L),
            integer(values, "tickId", -1L),
            integer(values, "scheduledStartNanos", -1L),
            integer(values, "startNanos", -1L),
            integer(values, "endNanos", -1L),
            integer(values, "missedPeriods", -1L),
            (int) integer(values, "catchUpTicks", 0L),
            integer(values, "skipped", -1L),
            integer(values, "executedNanos", -1L),
            epoch
        );
    }

    private static Fund fund(final Map<?, ?> values) {
        final long epoch = epoch(values.get("startTime"));
        if (epoch < 0L) {
            return null;
        }
        return new Fund(
            (int) integer(values, "minThreads", -1L),
            (int) integer(values, "liveThreads", -1L),
            (int) integer(values, "targetThreads", -1L),
            (int) integer(values, "maxThreads", -1L),
            (int) integer(values, "queuedTicks", -1L),
            (int) integer(values, "awaitingTicks", -1L),
            (int) integer(values, "executingTicks", -1L),
            (int) integer(values, "overdueTicks", -1L),
            integer(values, "spawned", -1L),
            integer(values, "retired", -1L),
            epoch
        );
    }

    private static long integer(final Map<?, ?> map, final String key, final long fallback) {
        return map.get(key) instanceof final Number number ? number.longValue() : fallback;
    }

    private static long epoch(final Object value) {
        if (!(value instanceof final String text)) {
            return -1L;
        }
        try {
            return OffsetDateTime.parse(text).toInstant().toEpochMilli();
        } catch (final DateTimeParseException failure) {
            return -1L;
        }
    }

    /**
     * Walks the {@code events} array and hands each event object to the consumer.
     *
     * <p>The document is consumed as a stream: an event object is collected between its braces and
     * parsed on its own, so memory stays proportional to one event instead of the whole recording.
     *
     * @param reader   JSON document reader
     * @param consumer event consumer
     * @throws IOException if the document cannot be read
     */
    private static void readEvents(final Reader reader, final java.util.function.Consumer<Map<String, Object>> consumer)
        throws IOException {
        final StringBuilder object = new StringBuilder(256);
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        boolean seenEvents = false;
        int current;
        while ((current = reader.read()) >= 0) {
            final char symbol = (char) current;
            if (!seenEvents) {
                if (symbol == '"') {
                    final StringBuilder name = new StringBuilder(8);
                    readStringTail(reader, name);
                    if (name.toString().equals("events")) {
                        seenEvents = true;
                    }
                }
                continue;
            }
            if (depth == 0) {
                if (symbol == '{') {
                    depth = 1;
                    object.setLength(0);
                    object.append(symbol);
                }
                continue;
            }
            object.append(symbol);
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (symbol == '\\') {
                    escaped = true;
                } else if (symbol == '"') {
                    inString = false;
                }
                continue;
            }
            switch (symbol) {
                case '"' -> inString = true;
                case '{' -> ++depth;
                case '}' -> {
                    --depth;
                    if (depth == 0) {
                        final Object parsed = Json.parse(object.toString());
                        if (parsed instanceof final Map<?, ?> map) {
                            @SuppressWarnings("unchecked")
                            final Map<String, Object> typed = (Map<String, Object>) map;
                            consumer.accept(typed);
                        }
                    }
                }
                default -> {
                    // whitespace and commas between events need no state
                }
            }
        }
    }

    private static void readStringTail(final Reader reader, final StringBuilder out) throws IOException {
        boolean escaped = false;
        int current;
        while ((current = reader.read()) >= 0) {
            final char symbol = (char) current;
            if (escaped) {
                out.append(symbol);
                escaped = false;
            } else if (symbol == '\\') {
                escaped = true;
            } else if (symbol == '"') {
                return;
            } else {
                out.append(symbol);
            }
        }
    }

    /**
     * One executed tick body.
     *
     * @param regionId            region of the tick, or {@code -1}
     * @param tickId              id of the scheduled tick
     * @param scheduledStartNanos deadline the body was measured against
     * @param startNanos          moment the body started
     * @param endNanos            moment the body returned
     * @param missedPeriods       whole periods the handle was behind, or {@code -1}
     * @param catchUpTicks        {@code 1} for a catch-up body
     * @param skipped             periods written off, or {@code -1}
     * @param executedNanos       wall clock spent in the body
     * @param epochMillis         event time, epoch milliseconds
     */
    record Tick(
        long regionId,
        long tickId,
        long scheduledStartNanos,
        long startNanos,
        long endNanos,
        long missedPeriods,
        int catchUpTicks,
        long skipped,
        long executedNanos,
        long epochMillis
    ) {

        /**
         * Returns whether the recorded deadline allows an exact lag.
         *
         * <p>The handle advances its scheduled start inside the body, so the value recorded after the
         * body is the next deadline (advanced by {@link #missedPeriods()} periods), unless the body
         * itself finished after that deadline. In the latter case the recorded value is the body end
         * and the original deadline is not recoverable from this event.
         *
         * @return {@code true} when the lag can be reconstructed exactly
         */
        boolean lagExact() {
            return this.missedPeriods >= 0L && this.endNanos <= this.scheduledStartNanos;
        }

        /**
         * Returns the lag of the body against the deadline it actually ran against.
         *
         * <p>Reconstruction: the recorded start is the deadline after the advance, so the deadline the
         * body ran against is {@code scheduledStartNanos - missedPeriods * period}. It is only valid
         * when {@link #lagExact()} holds; callers must not mix inexact rows into a distribution.
         *
         * @param periodNanos the tick period
         * @return nanoseconds late, never negative
         */
        long lagNanos(final long periodNanos) {
            return Math.max(0L, this.startNanos - this.scheduledStartNanos + this.missedPeriods * periodNanos);
        }

        /**
         * Returns whether this body counts as a hard miss.
         *
         * <p>The event's {@code missedPeriods} field is the handle's periods ahead at the body start, so
         * a value above one already means a whole period was lost. The exact lag is used as a second
         * signal when it is reconstructible.
         *
         * @param periodNanos the tick period
         * @return {@code true} when the lag reached a whole period or the handle was more than one
         *     period behind
         */
        boolean hardMiss(final long periodNanos) {
            return this.missedPeriods > 1L || (this.lagExact() && this.lagNanos(periodNanos) >= periodNanos);
        }
    }

    /**
     * One supervisor pass as the fund saw it.
     *
     * @param min          floor
     * @param live         live runners
     * @param target       size the pass asked for
     * @param max          ceiling
     * @param queued       ticks waiting for a permit
     * @param awaiting     runners waiting for a deadline
     * @param executing    runners inside a body
     * @param overdue      waiting ticks past the threshold
     * @param spawned      lifetime spawn counter
     * @param retired      lifetime retire counter
     * @param epochMillis  event time, epoch milliseconds
     */
    record Fund(
        int min,
        int live,
        int target,
        int max,
        int queued,
        int awaiting,
        int executing,
        int overdue,
        long spawned,
        long retired,
        long epochMillis
    ) {
    }

    /**
     * Events of one window, or the reason they are missing.
     *
     * @param ticks  executed ticks
     * @param fund   supervisor passes
     * @param reason why the events are missing, or the empty string
     */
    record Result(List<Tick> ticks, List<Fund> fund, String reason) {

        /**
         * Returns the value of an unreadable recording.
         *
         * @param reason why the events are missing
         * @return an absent result
         */
        static Result absent(final String reason) {
            return new Result(List.of(), List.of(), reason);
        }

        /**
         * Returns whether the recording produced per-tick data.
         *
         * @return {@code true} when at least one tick event was read
         */
        boolean present() {
            return !this.ticks.isEmpty();
        }
    }

    /**
     * Returns the {@code jfr} tool that belongs to the running JDK.
     *
     * @return absolute path to {@code jfr} or {@code jfr.exe}
     */
    static Path tool() {
        final String name = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win")
            ? "jfr.exe" : "jfr";
        return Path.of(System.getProperty("java.home"), "bin", name).toAbsolutePath();
    }
}
