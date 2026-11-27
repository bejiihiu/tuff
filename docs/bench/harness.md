# Harness reference

The harness is a single Java entry point, `kz.bejiihiu.tuff.bench.TuffBench`, in the
`tuff-benchmarks` module. It needs a JDK 25 (the same one that built the server) and a built paperclip
jar; it has no other dependency.

## Commands

### `run`

Executes one arm once and generates the per-run artifacts.

```bash
TuffBench run --jar <paperclip.jar> [options]
```

### `suite`

Executes `--blocks` runs of every selected arm in randomized order inside each block, then analyzes the
block. The plan is written to `plan.json` before the first run, so it cannot be reordered after seeing
the data. A finished and valid run is skipped on resume; `--force` redoes everything.

```bash
TuffBench suite --jar <paperclip.jar> --blocks 10 --arms A,A2,B,C
```

### `analyze`

Recomputes `summary.csv`, `compare.csv`, `summary.json` and `analysis.md` from the stored per-run
summaries, without starting a server.

```bash
TuffBench analyze --dir run/bench/<bench-id>
```

### `prepare`

Builds or refreshes the shared server template: a stopped server directory with a generated flat world,
the extracted libraries and the generated configuration files. The template is keyed on the jar hash and
the patch-set hash, so a new jar rebuilds it and a rerun reuses it. Downloaded libraries survive a
rebuild, which keeps the second build cheap and lets an offline machine reuse an existing cache.
`--rebuild` forces the world to be generated again.

### `self-check`

Exercises the pure parts of the harness with known inputs: JSON round trip and malformed input
rejection, percentile/median/deviation, bootstrap determinism, guardrail classification and the console
sample parser. It starts no server and is the cheapest check after a change to the analyzer.

```bash
TuffBench self-check
```

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--arm A\|A2\|B\|C` | `A` | arm of a single run |
| `--arms A,A2,B,C` | all four | arms of a block |
| `--baseline A` | `A` | baseline arm for the comparison |
| `--profile steady\|churn\|burst\|idle-compute` | `steady` | load shape |
| `--threads N` | `2` | region thread floor (`threaded-regions.threads`) |
| `--max-threads N` | `6` | Kairos ceiling for arm `C` |
| `--warmup S` | `600` | warm-up seconds, excluded from every statistic |
| `--measure S` | `1800` | measurement seconds |
| `--sample MS` | `5000` | console sampling interval |
| `--areas N` | `4` | force-loaded areas, 1 to 4 |
| `--area-chunks N` | `9` | side of one area in chunks |
| `--area-step N` | `1000` | distance between area origins in chunks |
| `--entities N` | `600` | entities summoned per area |
| `--entity <id>` | `villager` | entity type |
| `--heap 2G` | `2G` | `-Xms` and `-Xmx` of the server |
| `--out <dir>` | `run/bench` | artifact root |
| `--bench-id <id>` | derived | block identifier |
| `--run-id <id>` | derived | run identifier |
| `--port N` | `25670` | first server port; each run of a block gets its own |
| `--blocks N` | `10` | runs per arm in a suite |
| `--bootstrap N` | `10000` | bootstrap resamples |
| `--seed N` | `1337` | order and bootstrap seed |
| `--margin-hard-miss M` | 1% of baseline | absolute non-inferiority margin for the primary metric |
| `--margin-ratio R` | `0.02` | relative margin for the other metrics |
| `--force` | off | redo finished runs |
| `--rebuild` | off | regenerate the template world |
| `--no-tick-jfr` | off | do not arm `region-execution.metrics.jfr` (or the Kairos fund switch); the run then has no per-tick source |
| `--jvm "<flags>"` | empty | extra JVM flags |

Exit codes: `0` success and every run valid, `2` the harness could not do the work, `3` the work
finished but at least one run is invalid.

## What a run does

1. Copy the template into `runs/<run-id>/` (libraries, versions and caches are hard linked where the
   file system allows it; the world is a real copy).
2. Write `server.properties`, `eula.txt`, `config/tuff.yml` and patch the `threaded-regions` block of
   `config/paper-global.yml` for the arm. Nothing else in the configuration differs between arms.
3. Start the real paperclip jar as a child process with GC logging and a JFR recording. The manifest is
   written before the process starts, so a crash still leaves an explanation.
4. Wait for `Done (`, send the profile setup (game rules, force-loaded areas, entities), wait, bounded,
   until the region aggregate reports the planned number of regions.
5. Warm-up: sample the console every `--sample` milliseconds, applying the profile's periodic work.
6. Measurement: the same sampling, with the window boundaries recorded in the manifest.
7. Send `stop`, wait up to two minutes, capture the exit code and the stop duration. A process that does
   not leave is killed together with its descendants.
8. Parse the console and read the JFR recording for the measurement window, then run the guardrails.
9. Extract `tuff.scheduler.Tick` and `tuff.scheduler.Fund` with the JDK's `jfr print --json`, filter them
   to the measurement window, write `ticks.csv` and `fund.csv` from them, and render the summary and the
   report. Every arm arms the per-tick switch, so EDF, `WORK_STEALING` and Kairos all produce rows; an
   arm recorded with `--no-tick-jfr` keeps the fallback and says so.

The console sampling sends one `say BENCH-SAMPLE <seq> <phase>` marker followed by `tuff scheduler` and
`tuff threads`. The marker only delimits the answers of the two report commands; it does not influence
the scheduler.

## Metric definitions

All values are computed inside the measurement window only.

The primary metric has a true source and a fallback:

| Source | Definition |
| --- | --- |
| `jfr_per_tick` | a tick is a hard miss when its lag (`startNanos - scheduledStartNanos`) reaches one tick period (50 ms) or when the event's `missedPeriods` field is above 1 |
| `sampled_region_aggregate_proxy` | a 5 s sample is a hard miss when min region TPS < 19.5 or the worst tick of the window is at least 50 ms |

The field called `missedPeriods` in `tuff.scheduler.Tick` carries the handle's periods ahead, where 1
means on time; `skipped` is the number of periods the body wrote off. `skipped_periods` in the summary is
the sum of that field over the window.

The event records the deadline the body was measured against, captured before the handle advanced the
schedule. The lag is therefore the exact difference `startNanos - scheduledStartNanos`, with no
reconstruction, no period subtraction and no inexact rows. `lag_exact` remains in the artifact schema and
is always 1, so a reader of an older file still finds the column.

Every scheduler arm emits the event: EDF, `WORK_STEALING` and Kairos all go through the same region
handle seam. Arms A and its repeat A/A, B and C therefore all produce per-tick data. The analyzer
compares a pair of arms on the true metric whenever both produced ticks; the sampled aggregate is only
used when a run was made with recording off or its recording could not be read, and every row of
`summary.csv` carries a `source` column that names the definition used. A pair is never mixed.

| Metric | Definition |
| --- | --- |
| `hard_miss_share` | share of hard-miss ticks on the per-tick source, or of hard-miss samples on the fallback |
| `skipped_periods` | periods written off, per-tick source only |
| `lag_p50/p95/p99/p99.9/max` | distribution of per-tick lag, per-tick source only |
| `tick_mspt_p99_ns` | 99th percentile of per-tick body time, per-tick source only |
| `late_share` | share of samples where at least one region was lagging |
| `tps_min` / `tps_p50` | minimum and median of the lowest region TPS across samples |
| `mspt_avg_p50/p95/p99` | percentiles of the per-sample average MSPT, per region |
| `mspt_peak_p99/max` | percentile and maximum of the per-sample worst tick |
| `lag_pct_p50/p95` | percentiles of the lagging region share |
| `fund_live_median/max`, `fund_queued_max`, `fund_overdue_max` | fund telemetry, Kairos only |
| `spawn_delta`, `retire_delta` | fund growth and shrink inside the window |
| `thread_seconds` | sum over samples of live fund threads times the sample interval, Kairos only |
| `tick_origin` | scheduler reported by the per-tick event (`EDF`, `WORK_STEALING`, `KAIROS`); empty when the recording predates the field |
| `cpu_user`, `cpu_system` | mean JVM CPU load from `jdk.CPULoad` events |
| `gc_pause_ms`, `gc_pause_p99_ms` | pause time from `jdk.GCPhasePause` events |
| `heap_used_max_mb` | highest used heap observed at a GC in the window |
| `thread_starts`, `thread_ends` | `jdk.ThreadStart` / `jdk.ThreadEnd` counts |

Things the stand cannot observe, and says so in every report:

- player and packet load: regions come from force-loaded areas, not from clients;
- compute lane pressure: loading it needs a bench plugin.

`heap_used_max_mb` is only available when a GC happened in the window, because the JVM emits heap
summary events at collections. A run without a GC reports `n/a`, not zero.

## Configuration and reproducibility

Every run records in `manifest.json`:

- git head, branch, dirty flag and the changed files;
- a SHA-256 over every `.patch` file that makes up the build, plus the file count;
- the absolute jar path and its SHA-256;
- JDK version, vendor, VM and `java.home`;
- the exact JVM flag list and the full command line;
- CPU model, logical cores, total memory, OS name, version and architecture, host tag;
- SHA-256 of every configuration file the server read, plus a combined digest;
- the load geometry, the profile, and the planned and actual measurement window boundaries.

The block directory stores the same values once in `env.json`, so a later reader does not have to open
one run to learn what the block was.

## Statistics

The analyzer groups valid runs by arm and follows the pre-registered method:

- the unit of analysis is a run; ticks are never treated as independent samples;
- the difference of arm means is bootstrapped with 10 000 resamples and reported with its 95%
  percentile interval;
- the margin of a metric is `max(2 * sigma_A/A, policy delta)`, where `sigma_A/A` is the pooled run-to-run
  standard deviation of the two baseline runs (`A` and `A2`) on that metric; the policy delta is 1% of the
  baseline for the primary metric, 500 microseconds absolute for lag p99 and `--margin-ratio` for the
  others;
- non-inferiority is `upper bound of (arm - baseline) < margin` for a lower-is-better metric and
  `lower bound > -margin` for a higher-is-better one; superiority is the same interval entirely below
  (or above) zero;
- the noise band comes from the A/A pair. When both baseline runs produced per-tick rows the calibration
  uses the true metric, otherwise it uses the fallback and the report names the source; the row carries
  the source actually used;
- a block with no `A2` has no noise band, and the report marks every verdict provisional;
- a block with fewer than ten valid runs per arm is a pilot and the report says so.

The report also evaluates the pre-registered gates: `G1` (the port is non-inferior to the baseline on
the primary metric), `G2` (elasticity is a measurable improvement over the port) and `F1` (the port is
worse than the baseline, which stops the project).

## Artifact schema

`ticks.csv` - one row per executed tick, read from `tuff.scheduler.Tick`:

```text
run_id,seq,epoch_ms,elapsed_ms,region_id,tick_id,origin,scheduled_start_ns,start_ns,end_ns,
lag_ns,lag_exact,missed_periods,catch_up,skipped,executed_ns,hard_miss
```

`origin` is `EDF`, `WORK_STEALING` or `KAIROS` (empty only for a file written before the field existed),
`missed_periods` is the handle's periods ahead at the moment the body started (1 = on time), `skipped` is
the periods the body wrote off and `hard_miss` is 1 when the row meets the true definition. `lag_ns` is
`start_ns - scheduled_start_ns`, and `lag_exact` is always 1 because the deadline is recorded before the
handle advances. The file is header only when the arm has no per-tick source.

`fund.csv` - one row per supervisor pass, read from `tuff.scheduler.Fund`; when the arm has no fund
events the same columns are filled from the console samples and the last column says so:

```text
run_id,seq,epoch_ms,elapsed_ms,min,live,target,max,queued,awaiting,executing,overdue,
spawned,retired,source
```

`samples.csv` - the per-sample region aggregate from `/tuff threads`, used for the fallback metric and
for the per-region MSPT/TPS numbers:

```text
run_id,seq,phase,epoch_ms,elapsed_ms,regions,avg_mspt_5s,max_mspt_5s,avg_mspt_15s,
max_mspt_15s,min_tps,lag_pct,lagging_regions,entities,players,chunks
```

An unavailable field is empty in the CSV and `null` in the JSON, never zero.

`summary.csv` / `compare.csv`:

```text
metric,arm,baseline,source,n_arm,n_baseline,arm_mean,baseline_mean,delta,delta_rel,
ci_low,ci_high,margin,sigma,verdict
```

`source` is `jfr_per_tick` or `sampled_region_aggregate_proxy` and belongs to the row, not to the block:
the same metric can be true-sourced in one comparison and fallback-sourced in another. `verdict` is one
of `better`, `non_inferior`, `worse`, `inconclusive`, `noise_band` (the A/A row) or
`insufficient_data`.

## Platform notes

- Windows and Linux are both supported. The java executable is taken from the JDK that runs the harness
  (`java.exe` on Windows, `java` elsewhere), and a process tree is killed through
  `ProcessHandle.descendants()`.
- The server port is taken from `--port` and incremented per run; a block must have free ports in that
  range.
- The world is a flat world with seed `1337`, generated once in the template and copied into every run.
  Force-loaded areas are placed 1000 chunks apart so each becomes its own region.
- The stand never edits the server configuration of a running instance and never talks to a production
  server; it starts its own process in its own directory.
