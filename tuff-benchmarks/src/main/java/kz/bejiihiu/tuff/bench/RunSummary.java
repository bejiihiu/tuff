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
 * <p>{@code hard_miss_share} is the primary metric of this stand. It is computed from the real
 * per-sample region aggregate, not from invented ticks: a sample counts as a hard miss when the lowest
 * region TPS in its five second window is below 19.5 (the region lost at least 2.5% of its periods) or
 * when the worst single tick of the window reached a full 50 ms period. That is the closest observable
 * equivalent of "lag >= period or a period skipped"; the report names the source explicitly and the
 * analyzer prefers exact per-tick rows if a future collector provides them.
 */
record RunSummary(
    String runId,
    String arm,
    String profile,
    String scheduler,
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
    boolean valid
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
            Json.bool(map, "valid", false)
        );
    }
}
