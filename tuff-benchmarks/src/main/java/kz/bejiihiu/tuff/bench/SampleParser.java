package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns one captured console log into the samples the analyzer uses.
 *
 * <p>Parsing is a separate pass over the finished log on purpose: the measurement itself must not be
 * slowed down by the harness reading the same pipe the server writes, and a run whose log was cut
 * short still yields the samples that were actually printed. The marker command is the only thing the
 * harness injects, and its only job is to delimit the answer of the two report commands.
 *
 * <p>If a field is missing the sample keeps {@code NaN}/{@code -1}; nothing is interpolated.
 */
final class SampleParser {

    /** Console marker the sampler writes before the two report commands. */
    static final String MARKER = "BENCH-SAMPLE";

    private static final Pattern ANSI = Pattern.compile("\u001B\\[[0-9;]*m");
    private static final Pattern MARK = Pattern.compile(MARKER + " (\\d+) ([a-z-]+)");
    private static final Pattern FUND = Pattern.compile(
        "fund: min (\\d+), live (\\d+), target (\\d+), max (\\d+)"
    );
    private static final Pattern QUEUE = Pattern.compile(
        "queue: (\\d+) queued, (\\d+) awaiting, (\\d+) executing; tick wait avg ([\\d.]+) ms, "
            + "max ([\\d.]+) ms; overdue (\\d+) \\(threshold \\d+ ms(?:, oldest (\\d+) ms)?"
    );
    private static final Pattern EVENTS = Pattern.compile("fund events: spawned (\\d+), retired (\\d+)");
    private static final Pattern REGIONS = Pattern.compile(
        "regions (\\d+) / avg MSPT ([\\d.]+|unknown)(?: ms)? / min TPS ([\\d.]+|unknown) / lag (\\d+)% "
            + "\\((\\d+) of (\\d+)\\)"
    );
    private static final Pattern TICKS = Pattern.compile(
        "ticks: 5s max ([\\d.]+|unknown)(?: ms)?, 15s avg ([\\d.]+|unknown)(?: ms)? max "
            + "([\\d.]+|unknown)(?: ms)?; entities (\\d+) / players (\\d+) / chunks (\\d+)"
    );
    private static final Pattern SUPERVISOR = Pattern.compile("supervisor: ([a-z ()\\-]+);");
    private static final Pattern STATUS = Pattern.compile("- status: ([a-z]+)");
    private static final Pattern SCHEDULER_NAME = Pattern.compile("^\\s*scheduler: (\\w+)");
    private static final Pattern DEGRADED = Pattern.compile("degraded: (.+)$");
    private static final Pattern TUFF_SCHEDULER = Pattern.compile("Tuff scheduler: (\\w+)");

    private SampleParser() {
    }

    /**
     * Parses a captured log against the marks the sampler wrote.
     *
     * @param log   captured console log
     * @param marks marks, in send order
     * @return samples in mark order plus the raw block of each mark
     * @throws IOException if the log cannot be read
     */
    static Result parse(final Path log, final List<SampleMark> marks) throws IOException {
        final Map<Long, Mutable> bySeq = new LinkedHashMap<>();
        for (final SampleMark mark : marks) {
            bySeq.put(mark.seq(), new Mutable());
        }
        Mutable current = null;
        for (final String rawLine : Files.readAllLines(log, StandardCharsets.UTF_8)) {
            final String line = ANSI.matcher(rawLine).replaceAll("");
            final Matcher mark = MARK.matcher(line);
            if (mark.find()) {
                final long seq = Long.parseLong(mark.group(1));
                current = bySeq.get(seq);
                if (current != null) {
                    current.raw.add(line);
                }
                continue;
            }
            if (current == null) {
                continue;
            }
            current.raw.add(line);
            current.accept(line);
        }
        final List<Sample> samples = new ArrayList<>(marks.size());
        final Map<Long, List<String>> raw = new LinkedHashMap<>();
        for (final SampleMark mark : marks) {
            final Mutable parsed = bySeq.getOrDefault(mark.seq(), new Mutable());
            samples.add(new Sample(
                mark.seq(),
                mark.phase(),
                mark.epochMillis(),
                mark.elapsedMillis(),
                parsed.region(),
                parsed.fund(),
                parsed.scheduler(),
                parsed.raw
            ));
            raw.put(mark.seq(), List.copyOf(parsed.raw));
        }
        return new Result(List.copyOf(samples), Map.copyOf(raw));
    }

    /**
     * One sample's send metadata, written before the answers arrive.
     *
     * @param seq          marker sequence
     * @param phase        {@code warmup}, {@code measure} or {@code last}
     * @param epochMillis  wall clock at send time
     * @param elapsedMillis time since the run start marker
     */
    record SampleMark(long seq, String phase, long epochMillis, long elapsedMillis) {
    }

    /**
     * Parsed samples and their raw console blocks.
     *
     * @param samples samples in mark order
     * @param raw     raw block per sequence
     */
    record Result(List<Sample> samples, Map<Long, List<String>> raw) {
    }

    /**
     * Mutable accumulator for one block; the server prints the fields in any order.
     */
    private static final class Mutable {

        private int regionCount = -1;
        private double avgMspt5s = Double.NaN;
        private double maxMspt5s = Double.NaN;
        private double avgMspt15s = Double.NaN;
        private double maxMspt15s = Double.NaN;
        private double minTps = Double.NaN;
        private int lagPercent = -1;
        private int laggingRegions = -1;
        private long entities = -1L;
        private long players = -1L;
        private long chunks = -1L;

        private int fundMin = -1;
        private int fundLive = -1;
        private int fundTarget = -1;
        private int fundMax = -1;
        private int queued = -1;
        private int awaiting = -1;
        private int executing = -1;
        private int overdue = -1;
        private long oldestOverdueMs = -1L;
        private double waitAverageMs = Double.NaN;
        private double waitMaximumMs = Double.NaN;
        private long spawned = -1L;
        private long retired = -1L;

        private String name = "";
        private String supervisor = "";
        private String status = "";
        private String degraded = "";

        private final List<String> raw = new ArrayList<>(32);

        private void accept(final String line) {
            Matcher match = FUND.matcher(line);
            if (match.find()) {
                this.fundMin = Integer.parseInt(match.group(1));
                this.fundLive = Integer.parseInt(match.group(2));
                this.fundTarget = Integer.parseInt(match.group(3));
                this.fundMax = Integer.parseInt(match.group(4));
            }
            match = QUEUE.matcher(line);
            if (match.find()) {
                this.queued = Integer.parseInt(match.group(1));
                this.awaiting = Integer.parseInt(match.group(2));
                this.executing = Integer.parseInt(match.group(3));
                this.waitAverageMs = Double.parseDouble(match.group(4));
                this.waitMaximumMs = Double.parseDouble(match.group(5));
                this.overdue = Integer.parseInt(match.group(6));
                this.oldestOverdueMs = match.group(7) == null ? 0L : Long.parseLong(match.group(7));
            }
            match = EVENTS.matcher(line);
            if (match.find()) {
                this.spawned = Long.parseLong(match.group(1));
                this.retired = Long.parseLong(match.group(2));
            }
            match = REGIONS.matcher(line);
            if (match.find()) {
                this.regionCount = Integer.parseInt(match.group(1));
                this.avgMspt5s = number(match.group(2));
                this.minTps = number(match.group(3));
                this.lagPercent = Integer.parseInt(match.group(4));
                this.laggingRegions = Integer.parseInt(match.group(5));
            }
            match = TICKS.matcher(line);
            if (match.find()) {
                this.maxMspt5s = number(match.group(1));
                this.avgMspt15s = number(match.group(2));
                this.maxMspt15s = number(match.group(3));
                this.entities = Long.parseLong(match.group(4));
                this.players = Long.parseLong(match.group(5));
                this.chunks = Long.parseLong(match.group(6));
            }
            match = SUPERVISOR.matcher(line);
            if (match.find()) {
                this.supervisor = match.group(1).strip();
            }
            match = STATUS.matcher(line);
            if (match.find()) {
                this.status = match.group(1);
            }
            match = SCHEDULER_NAME.matcher(line);
            if (match.find() && this.name.isEmpty()) {
                this.name = match.group(1);
            }
            match = TUFF_SCHEDULER.matcher(line);
            if (match.find() && this.name.isEmpty()) {
                this.name = match.group(1);
            }
            match = DEGRADED.matcher(line);
            if (match.find()) {
                this.degraded = match.group(1).strip();
            }
        }

        private Sample.RegionSample region() {
            if (this.regionCount < 0) {
                return Sample.RegionSample.ABSENT;
            }
            return new Sample.RegionSample(
                this.regionCount,
                this.avgMspt5s,
                this.maxMspt5s,
                this.avgMspt15s,
                this.maxMspt15s,
                this.minTps,
                this.lagPercent,
                this.laggingRegions,
                this.entities,
                this.players,
                this.chunks
            );
        }

        private Sample.FundSample fund() {
            if (this.fundMin < 0) {
                return Sample.FundSample.ABSENT;
            }
            return new Sample.FundSample(
                this.fundMin,
                this.fundLive,
                this.fundTarget,
                this.fundMax,
                this.queued,
                this.awaiting,
                this.executing,
                this.overdue,
                this.oldestOverdueMs,
                this.waitAverageMs,
                this.waitMaximumMs,
                this.spawned,
                this.retired
            );
        }

        private Sample.SchedulerSample scheduler() {
            return new Sample.SchedulerSample(this.name, this.supervisor, this.status, this.degraded);
        }

        private static double number(final String value) {
            return "unknown".equals(value) ? Double.NaN : Double.parseDouble(value);
        }
    }
}
