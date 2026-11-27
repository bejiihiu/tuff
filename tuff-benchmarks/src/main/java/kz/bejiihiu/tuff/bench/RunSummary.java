package kz.bejiihiu.tuff.bench;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One run reduced to the numbers the analyzer compares.
 *
 * <p>The reduction happens inside the sample window only, and it is stored as {@code summary.json}
 * next to the raw samples so that the suite report can be recomputed without repeating the run.
 *
 * <p>{@code hard_miss_share} is the primary metric of this stand. When the run produced the scheduler's
 * own per-tick events it is the true share: a tick counts as a hard miss when its lag reached one tick
 * period or when the handle was more than one period behind. Otherwise the run falls back to the
 * sampled region aggregate, where a sample counts as a hard miss when the lowest region TPS is below
 * 19.5 or the worst tick of the window reached 50 ms. The summary names the source, and the analyzer
 * refuses to compare arms that were measured by different sources.
 */
record RunSummary(
    String runId,
    String arm,
    String profile,
    String scheduler,
    String tickOrigin,
    boolean elastic,
    int threads,
    int maxThreads,
    String benchId,
    String gitHead,
    String jarSha256,
    String jdk,
    String hostTag,
    int warmupSeconds,
    int measureSeconds,
    int sampleMillis,
    String hardMissSource,
    String hardMissDefinition,
    int samples,
    double regionsMedian,
    double entitiesMedian,
    double playersMedian,
    double chunksMedian,
    int hardMissSamples,
    double hardMissShare,
    double lateShare,
    double tpsMin,
    double tpsP50,
    double msptAverageP50,
    double msptAverageP95,
    double msptAverageP99,
    double msptPeakP99,
    double msptPeakMax,
    double lagPercentP50,
    double lagPercentP95,
    double fundLiveMedian,
    double fundLiveMax,
    double fundQueuedMax,
    double fundOverdueMax,
    double spawnDelta,
    double retireDelta,
    double threadSeconds,
    boolean jfrPresent,
    double cpuUser,
    double cpuSystem,
    double gcPauseMillis,
    double gcPauseP99Millis,
    double heapUsedMaxMb,
    double threadStarts,
    double threadEnds,
    int exitCode,
    double stopSeconds,
    boolean guardrailOk,
    List<String> invalidReasons,
    boolean valid,
    boolean perTickRequested,
    int ticks,
    int hardMissTicks,
    double proxyHardMissShare,
    long skippedPeriods,
    long catchUpTicks,
    long tickPeriodNanos,
    double lagP50Nanos,
    double lagP95Nanos,
    double lagP99Nanos,
    double lagP999Nanos,
    double lagMaxNanos,
    double tickMsptP99Nanos,
    int fundPasses,
    int lagInexactTicks
) {

    /**
     * Freezes the invalid reason list.
     */
    RunSummary {
        invalidReasons = List.copyOf(invalidReasons);
    }

    /**
     * Serialises the summary into the flat artifact schema.
     *
     * @return ordered map with snake case keys
     */
    Map<String, Object> toMap() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("run_id", this.runId);
        map.put("arm", this.arm);
        map.put("profile", this.profile);
        map.put("scheduler", this.scheduler);
        map.put("tick_origin", this.tickOrigin);
        map.put("elastic", this.elastic);
        map.put("threads", this.threads);
        map.put("max_threads", this.maxThreads);
        map.put("bench_id", this.benchId);
        map.put("git_head", this.gitHead);
        map.put("jar_sha256", this.jarSha256);
        map.put("jdk", this.jdk);
        map.put("host_tag", this.hostTag);
        map.put("warmup_s", this.warmupSeconds);
        map.put("measure_s", this.measureSeconds);
        map.put("sample_ms", this.sampleMillis);
        map.put("hard_miss_source", this.hardMissSource);
        map.put("hard_miss_definition", this.hardMissDefinition);
        map.put("samples", this.samples);
        map.put("regions_median", this.regionsMedian);
        map.put("entities_median", this.entitiesMedian);
        map.put("players_median", this.playersMedian);
        map.put("chunks_median", this.chunksMedian);
        map.put("hard_miss_samples", this.hardMissSamples);
        map.put("hard_miss_share", this.hardMissShare);
        map.put("late_share", this.lateShare);
        map.put("tps_min", this.tpsMin);
        map.put("tps_p50", this.tpsP50);
        map.put("mspt_avg_p50", this.msptAverageP50);
        map.put("mspt_avg_p95", this.msptAverageP95);
        map.put("mspt_avg_p99", this.msptAverageP99);
        map.put("mspt_peak_p99", this.msptPeakP99);
        map.put("mspt_peak_max", this.msptPeakMax);
        map.put("lag_pct_p50", this.lagPercentP50);
        map.put("lag_pct_p95", this.lagPercentP95);
        map.put("fund_live_median", this.fundLiveMedian);
        map.put("fund_live_max", this.fundLiveMax);
        map.put("fund_queued_max", this.fundQueuedMax);
        map.put("fund_overdue_max", this.fundOverdueMax);
        map.put("spawn_delta", this.spawnDelta);
        map.put("retire_delta", this.retireDelta);
        map.put("thread_seconds", this.threadSeconds);
        map.put("jfr_present", this.jfrPresent);
        map.put("cpu_user", this.cpuUser);
        map.put("cpu_system", this.cpuSystem);
        map.put("gc_pause_ms", this.gcPauseMillis);
        map.put("gc_pause_p99_ms", this.gcPauseP99Millis);
        map.put("heap_used_max_mb", this.heapUsedMaxMb);
        map.put("thread_starts", this.threadStarts);
        map.put("thread_ends", this.threadEnds);
        map.put("exit_code", this.exitCode);
        map.put("stop_seconds", this.stopSeconds);
        map.put("guardrail_ok", this.guardrailOk);
        map.put("invalid_reasons", new ArrayList<>(this.invalidReasons));
        map.put("valid", this.valid);
        map.put("per_tick_requested", this.perTickRequested);
        map.put("ticks", this.ticks);
        map.put("hard_miss_ticks", this.hardMissTicks);
        map.put("proxy_hard_miss_share", this.proxyHardMissShare);
        map.put("skipped_periods", this.skippedPeriods);
        map.put("catch_up_ticks", this.catchUpTicks);
        map.put("tick_period_ns", this.tickPeriodNanos);
        map.put("lag_p50_ns", this.lagP50Nanos);
        map.put("lag_p95_ns", this.lagP95Nanos);
        map.put("lag_p99_ns", this.lagP99Nanos);
        map.put("lag_p99_9_ns", this.lagP999Nanos);
        map.put("lag_max_ns", this.lagMaxNanos);
        map.put("tick_mspt_p99_ns", this.tickMsptP99Nanos);
        map.put("fund_passes", this.fundPasses);
        map.put("lag_inexact_ticks", this.lagInexactTicks);
        return map;
    }

    /**
     * Reads a stored summary.
     *
     * @param map parsed JSON object
     * @return the typed summary
     */
    static RunSummary fromMap(final Map<String, Object> map) {
        @SuppressWarnings("unchecked")
        final List<String> reasons = map.get("invalid_reasons") instanceof final List<?> list
            ? list.stream().map(String::valueOf).toList()
            : List.of();
        return new RunSummary(
            Json.string(map, "run_id", ""),
            Json.string(map, "arm", ""),
            Json.string(map, "profile", ""),
            Json.string(map, "scheduler", ""),
            Json.string(map, "tick_origin", ""),
            Json.bool(map, "elastic", false),
            (int) Json.integer(map, "threads", -1),
            (int) Json.integer(map, "max_threads", -1),
            Json.string(map, "bench_id", ""),
            Json.string(map, "git_head", ""),
            Json.string(map, "jar_sha256", ""),
            Json.string(map, "jdk", ""),
            Json.string(map, "host_tag", ""),
            (int) Json.integer(map, "warmup_s", -1),
            (int) Json.integer(map, "measure_s", -1),
            (int) Json.integer(map, "sample_ms", -1),
            Json.string(map, "hard_miss_source", ""),
            Json.string(map, "hard_miss_definition", ""),
            (int) Json.integer(map, "samples", 0),
            Json.number(map, "regions_median", Double.NaN),
            Json.number(map, "entities_median", Double.NaN),
            Json.number(map, "players_median", Double.NaN),
            Json.number(map, "chunks_median", Double.NaN),
            (int) Json.integer(map, "hard_miss_samples", 0),
            Json.number(map, "hard_miss_share", Double.NaN),
            Json.number(map, "late_share", Double.NaN),
            Json.number(map, "tps_min", Double.NaN),
            Json.number(map, "tps_p50", Double.NaN),
            Json.number(map, "mspt_avg_p50", Double.NaN),
            Json.number(map, "mspt_avg_p95", Double.NaN),
            Json.number(map, "mspt_avg_p99", Double.NaN),
            Json.number(map, "mspt_peak_p99", Double.NaN),
            Json.number(map, "mspt_peak_max", Double.NaN),
            Json.number(map, "lag_pct_p50", Double.NaN),
            Json.number(map, "lag_pct_p95", Double.NaN),
            Json.number(map, "fund_live_median", Double.NaN),
            Json.number(map, "fund_live_max", Double.NaN),
            Json.number(map, "fund_queued_max", Double.NaN),
            Json.number(map, "fund_overdue_max", Double.NaN),
            Json.number(map, "spawn_delta", Double.NaN),
            Json.number(map, "retire_delta", Double.NaN),
            Json.number(map, "thread_seconds", Double.NaN),
            Json.bool(map, "jfr_present", false),
            Json.number(map, "cpu_user", Double.NaN),
            Json.number(map, "cpu_system", Double.NaN),
            Json.number(map, "gc_pause_ms", Double.NaN),
            Json.number(map, "gc_pause_p99_ms", Double.NaN),
            Json.number(map, "heap_used_max_mb", Double.NaN),
            Json.number(map, "thread_starts", Double.NaN),
            Json.number(map, "thread_ends", Double.NaN),
            (int) Json.integer(map, "exit_code", -1),
            Json.number(map, "stop_seconds", Double.NaN),
            Json.bool(map, "guardrail_ok", false),
            reasons,
            Json.bool(map, "valid", false),
            Json.bool(map, "per_tick_requested", false),
            (int) Json.integer(map, "ticks", 0),
            (int) Json.integer(map, "hard_miss_ticks", 0),
            Json.number(map, "proxy_hard_miss_share", Double.NaN),
            Json.integer(map, "skipped_periods", 0L),
            Json.integer(map, "catch_up_ticks", 0L),
            Json.integer(map, "tick_period_ns", JfrTicks.TICK_PERIOD_NANOS),
            Json.number(map, "lag_p50_ns", Double.NaN),
            Json.number(map, "lag_p95_ns", Double.NaN),
            Json.number(map, "lag_p99_ns", Double.NaN),
            Json.number(map, "lag_p99_9_ns", Double.NaN),
            Json.number(map, "lag_max_ns", Double.NaN),
            Json.number(map, "tick_mspt_p99_ns", Double.NaN),
            (int) Json.integer(map, "fund_passes", 0),
            (int) Json.integer(map, "lag_inexact_ticks", 0)
        );
    }
}
