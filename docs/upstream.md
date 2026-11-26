# Updating from upstream

Tuff tracks [Folia](https://github.com/PaperMC/Folia) and nothing else. This page is the procedure for
pulling a new upstream state into `ver/26.3.x` without losing the patch stack, the branding or the
compute hooks.

## Upstream model

| Remote | URL | Role |
| --- | --- | --- |
| `origin` | `https://github.com/bejiihiu/tuff.git` | our fork, `ver/26.3.x` is the integration branch |
| `upstream` | `https://github.com/PaperMC/Folia.git` | the only upstream we merge |

Paper is **not** a remote here. Folia pins a Paper commit in `gradle.properties`:

```properties
paperRef=827eeeb9077c4cc7231eaacb7ffcf597d4fedd9f
```

`paperweight` clones that commit during `applyAllPatches`, applies Folia's Paper patches, then ours. When
Folia bumps `paperRef`, the Minecraft update arrives with the Folia merge; nobody edits `paperRef` by hand.

The fork renames Folia's modules and keeps its patch stack in the same layout, so the upstream → Tuff
path mapping is:

| Folia | Tuff |
| --- | --- |
| `folia-api/paper-patches/` | `tuff-api/paper-patches/` |
| `folia-server/paper-patches/` | `tuff-server/paper-patches/` |
| `folia-server/minecraft-patches/` | `tuff-server/minecraft-patches/` |
| `folia-checkstyle/paper-patches/`, `folia-checkstyle/config-patches/` | `tuff-checkstyle/paper-patches/`, `tuff-checkstyle/config-patches/` |
| `folia-*/build.gradle.kts.patch` | `tuff-*/build.gradle.kts.patch` |

Patch bodies under those directories are Folia's, byte for byte. The only upstream files this fork
rewrites are the three `tuff-*/build.gradle.kts.patch` diffs (module names and paths); everything else is
untouched. Our own patches are appended after Folia's:

| Patch | Layer | What it does |
| --- | --- | --- |
| `tuff-server/paper-patches/features/0008-Tuff-rebrand.patch` | Paper server | brand fallback, bStats off, `TuffVersionFetcher` |
| `tuff-server/paper-patches/features/0009-Tuff-bootstrap-the-compute-engine-and-its-command.patch` | Paper server | `CraftServer` bootstrap, `/tuff` in `PaperCommands` |
| `tuff-server/minecraft-patches/features/0015-Tuff-drive-the-compute-engine-lifecycle.patch` | NMS | budget after region init, stop before the world save |
| `tuff-server/minecraft-patches/features/0016-Tuff-account-region-tick-bodies.patch` | NMS | one call in Folia's region thread factory: the tick body is wrapped for accounting when `region-execution.managed` is on |

## Before you start

- Git and a JDK; Gradle resolves JDK 25 through the toolchain.
- `git config --global core.autocrlf false`. With CRLF in the generated sources checkstyle fails on
  `LineEnding` before you get anywhere near the upstream diff.
- No half-finished work in the generated trees: `git -C paper-server status`, `git -C paper-api status`,
  `git -C tuff-server/src/minecraft/java status`. They are ignored by the root repository and get
  re-created, but a dirty or stale tree makes `applyAllPatches` fail with unrelated errors.
- A working baseline: `./gradlew cleanCache applyAllPatches` and `./gradlew build` green on the current
  commit, so a failure after the merge has one cause.

## Routine update

```bash
git fetch upstream
git switch ver/26.3.x

git log --oneline HEAD..upstream/ver/26.3.x          # what arrives
git diff --stat HEAD...upstream/ver/26.3.x           # how big it is
```

Merge, do not rebase. The branch is public, our patch files are derived from committed state, and a
merge keeps the history that the patch stack was built on:

```bash
git merge upstream/ver/26.3.x
```

Rebase is only reasonable on a throwaway local copy; it rewrites published commits and forces a
re-apply of the whole stack for no benefit.

After the merge:

1. Resolve the conflicts listed below. Keep Tuff's module names, `group`, publishing block and
   workflows; take upstream's `paperRef`, `mcVersion`, `apiVersion` and Paper/Folia code.
2. Check that no upstream path leaked into the tree:
   ```bash
   git ls-files 'folia-*'
   ```
   Git adds new upstream files at the path they were created at, so a brand-new Folia patch can land in
   `folia-server/...` (a directory this fork no longer has) instead of `tuff-server/...`. Move it to the
   matching directory, keep the file name, and make sure it sorts before our patches.
3. Renumber our patches if Folia took a number (see below).
4. Apply from zero and let the tool tell you whether the stack still fits:
   ```bash
   ./gradlew cleanCache applyAllPatches
   ```
5. Regenerate all derived patches and inspect what changed:
   ```bash
   ./rb.sh          # Windows: rb.bat
   git status
   ```
   Only the patch files you meant to touch should appear. If half the stack shows up as modified, a
   commit landed on a different base or a `.patch` was edited by hand — discard and start from the
   applied sources again.
6. Build and test, then run the runtime checks in [Verification](#verification-after-an-update).
7. Commit the merge plus any patch regeneration as one change, push the branch, and start CI by hand
   (pushes to `ver/26.3.x` are ignored by `build.yml`):
   ```bash
   gh workflow run build.yml --ref ver/26.3.x -R bejiihiu/tuff
   ```
   `-R bejiihiu/tuff` is not optional: this clone has two remotes and `gh` otherwise picks Folia.

### When a patch does not apply

`applyAllPatches` stops at the first patch that does not fit and names it. The applied tree contains
every patch before it, so that is the base to re-author against:

1. Open the file the patch touches in the applied sources (`paper-server/src/main/java/...`,
   `tuff-server/src/minecraft/java/...`, `paper-api/src/main/java/...`).
2. Re-apply the *intent* of the patch to the new code, keeping the same subject line
   (`Tuff: <what it does>`).
3. Commit it in the applied repository and rebuild that layer:
   ```bash
   git -C paper-server commit -am "Tuff: hook whatever into CraftServer"
   ./gradlew :tuff-server:rebuildPaperServerFeaturePatches
   ```
4. `./gradlew cleanCache applyAllPatches` again — a patch is only fixed when it applies from zero.

`.rej` files, if the tool wrote any next to the source, are diagnostics, not input. Never edit a
generated `.patch` to make it apply; the next rebuild overwrites it from the applied commits and the
edit disappears silently. The three `tuff-*/build.gradle.kts.patch` files are the exception — they are
plain diffs maintained by hand, because the files they patch (`paper-*/build.gradle.kts`) are generated.

## Expected conflicts

| Path | Why it conflicts | Resolution |
| --- | --- | --- |
| `settings.gradle.kts` | root project, renamed modules, `tuff-compute` / `tuff-benchmarks` includes | keep our module list and version block; take upstream additions |
| `gradle.properties` | `group=kz.bejiihiu.tuff` vs `dev.folia`; upstream moves `mcVersion`, `apiVersion`, `paperRef` | keep our `group` and `channel`; take upstream's Minecraft and Paper values |
| `build.gradle.kts` | our `forks.register("tuff")` block, `patchFile` mappings, publishing repository | keep the fork block and publishing; take upstream's paperweight/plugin version bumps |
| `.gitignore` | generated directories were renamed (`/paper-server`, `/tuff-server/src/minecraft`, `tuff-*/build.gradle.kts`) | keep our entries, add anything upstream started generating |
| `tuff-api/build.gradle.kts.patch`, `tuff-server/build.gradle.kts.patch`, `tuff-checkstyle/build.gradle.kts.patch` | hand-maintained diffs against `paper-*/build.gradle.kts`; upstream changes hit the same hunks | reconcile hunk by hunk, then prove it with `cleanCache applyAllPatches` |
| `.github/workflows/build.yml`, `release.yml` | ours replaced Folia's; upstream keeps `release-26.3.yaml`, which we deleted | keep ours, keep `release-26.3.yaml` deleted |
| `tuff-server/paper-patches/features/0008-*`, `0009-*`, `tuff-server/minecraft-patches/features/0015-*` | our own patches; the merge does not conflict, the **apply** fails when upstream moved the surrounding code | re-author in the applied sources, rebuild the layer |
| `folia-*/...` | new upstream files keep their old path | move into the matching `tuff-*` directory |

Folia's own patch bodies do not conflict: we do not edit them, so a merge takes upstream's version
wholesale. The failure mode there is a path leak (row 8), not a content conflict.

## Our patches go last

Patch files apply in file-name order, and each one becomes one commit in the applied repository. Rebuild
numbers them sequentially over the commits, so the number is a result of the position, not something to
preserve:

- If Folia adds `0015-Some-fix.patch` in `tuff-server/minecraft-patches/features/` and ours is
  `0015-Tuff-drive-the-compute-engine-lifecycle.patch`, rename ours to the next free number
  (`git mv ... 0016-...`), so Folia's patch applies first.
- Do not renumber Folia's patches and do not edit their content.
- After the rename run `./gradlew cleanCache applyAllPatches` and then `./rb.sh`; the rebuild rewrites
  the canonical file name from the commit order, and `git status` shows the rename.
- Same rule for `0008`/`0009` in `tuff-server/paper-patches/features/`: our two patches stay at the tail.

A gap in the numbering is harmless. A duplicate is not — two patches with the same prefix apply in an
order decided by the rest of the name, which is not the order you meant.

## Invocation points in server code

Three places call into Tuff. They are the only reason the server knows the engine exists, and an
upstream refactor of these methods is the most likely way an update breaks at runtime while still
compiling.

| File | Hook | Required position |
| --- | --- | --- |
| `paper-server/src/main/java/org/bukkit/craftbukkit/CraftServer.java` | `ComputeBootstrap.start()` | end of the constructor: command map, plugin managers and Bukkit config exist, plugins are not enabled yet |
| `paper-server/src/main/java/io/papermc/paper/command/PaperCommands.java` | `COMMANDS.put("tuff", ...)` | next to the other Paper/Folia commands, before the map is registered |
| `tuff-server/src/minecraft/java/net/minecraft/server/MinecraftServer.java` | `ComputeBootstrap.applyRegionBudget()` | inside `runServer`, immediately after `RegionizedServer.getInstance().init()`; region, chunk and IO thread counts are final only there |
| `tuff-server/src/minecraft/java/net/minecraft/server/MinecraftServer.java` | `ComputeBootstrap.stop()` | in `stopServer`, after `waitForAsyncTasksShutdown()` and before the world save |

Check the applied sources after every upstream update:

```bash
grep -n "Tuff - start the compute engine" paper-server/src/main/java/org/bukkit/craftbukkit/CraftServer.java
grep -n 'COMMANDS.put("tuff"' paper-server/src/main/java/io/papermc/paper/command/PaperCommands.java
grep -n "Tuff - size the compute pool\|Tuff - stop the compute engine" \
  tuff-server/src/minecraft/java/net/minecraft/server/MinecraftServer.java
```

A hit only proves the line survived. Read the diff around it: the same call in a constructor that now
runs before the plugin manager exists, or a `stop()` moved behind the world save, compiles and starts
fine and is still wrong.

`ComputeBootstrap.applyRegionBudget()` reads
`io.papermc.paper.threadedregions.TickRegions#getScheduler()`, `TickRegionScheduler#getTotalThreadCount()`
and `ca.spottedleaf.moonrise.common.util.MoonriseCommon.WORKER_POOL` / `IO_POOL`. A rename there is a
compile error in `tuff-server`, which is the cheap outcome; a behaviour change is a startup WARN
(`Could not read the server thread counts; ...`) and a pool stuck at its startup size.

## Verification after an update

### Build and tests

```bash
./gradlew cleanCache applyAllPatches
./gradlew build
```

`build` runs `check` for every module: checkstyle, JUnit, and `scanJarForBadCalls` on the server jar.
Targeted re-runs when something fails:

```bash
./gradlew :tuff-compute:test :tuff-api:test :tuff-server:test
./gradlew :tuff-compute:javadoc :tuff-api:javadoc
```

Two things about the server tests: `tuff-server` only runs classes matching `**/**TestSuite.class`
(`ComputeBootstrapTestSuite`, `TuffComputeConfigTestSuite`), with `forkEvery = 1` and the `Slow` tag
excluded. And a green build with zero executed compute tests is not evidence: CI fails in that case, so
check the JUnit XML under `tuff-compute/build/test-results/test` if you suspect it.

### Artifacts

```bash
./gradlew :tuff-server:createPaperclipJar
```

The runnable artifact is `tuff-server/build/libs/tuff-paperclip-<version>.jar`. `build` alone does not
produce it.

Brand, from the server jar:

```bash
jar xf tuff-server/build/libs/tuff-server-*.jar META-INF/MANIFEST.MF
grep -E 'Brand-(Name|Id)|Implementation-Title' META-INF/MANIFEST.MF
rm -rf META-INF
```

Expect `Brand-Name: Tuff`, `Brand-Id: kz.bejiihiu:tuff`, `Implementation-Title: Tuff`.

Engine inside the paperclip jar. `tuff-compute` and `tuff-api` ship as paperclip libraries, so the
classes are one archive deeper than the paperclip root:

```bash
jar xf tuff-server/build/libs/tuff-paperclip-*.jar META-INF/libraries/kz
jar tf META-INF/libraries/kz/bejiihiu/tuff/tuff-compute/*/tuff-compute-*.jar \
  | grep '^kz/bejiihiu/tuff/compute/internal/engine/'
rm -rf META-INF
```

An empty list means the engine was dropped from the bundle — a build-config regression, not an
upstream bug.

### Smoke run, engine enabled

Run it outside the repository; paperclip writes its cache and the world into the working directory
(`/tmp/tuff-smoke` below, any scratch directory on Windows).

```bash
mkdir -p /tmp/tuff-smoke && cd /tmp/tuff-smoke
echo "eula=true" > eula.txt
java -Xms512M -Xmx2G -jar /path/to/tuff-server/build/libs/tuff-paperclip-*.jar --nogui
```

Useful minimum in `server.properties`: `online-mode=false`, `level-type=flat`, `max-players=1`,
`view-distance=4`, `simulation-distance=4`, a non-default `port`.

Expect, in this order:

```
[Tuff/ComputeConfig] Created the default compute configuration at ...
[Tuff/ComputeConfig] Compute configuration loaded from ...
[Tuff/Compute] Tuff compute enabled: <N> worker(s) (...)
[io.papermc.paper.threadedregions.TickRegions] Initialised EDF Folia scheduler with initial <N> target thread(s)
[Tuff/Compute] Tuff compute: processor budget <B> of <C> (<r> region, <k> chunk, <i> IO threads)
Done (...)! For help, type "help"
```

Then from the console:

- `tuff` — summary, lane line, queue, counters, settings; no stack traces.
- `tuff compute consumers` — the example policies plus `default`.
- `tuff compute jfr start` — refuses on the default config because `async-compute.metrics.jfr` is
  `false`; set it to `true` and restart to exercise the recording path.
- `stop` — `Tuff compute stopped in N ms` before the world save, exit code 0, no
  `did not terminate` / ERROR from `Tuff/Compute`.

`./gradlew :tuff-server:runPaperclip` is an alternative launcher (working directory `run/` by default,
`-Ppaper.runWorkDir=...` to change it). The `run*` tasks pin a JetBrains Runtime 25 toolchain, so they
fail during configuration on a machine that only has a Temurin JDK; they do not create `eula.txt` either.

### Smoke run, engine disabled

Set `async-compute.enabled: false` in `config/tuff.yml`, start again, and check:

- `Tuff compute is disabled in <file> (async-compute.enabled: false)` at startup;
- `tuff` answers with that message instead of an error;
- `jcmd <pid> Thread.print` shows no `Tuff Compute` threads — the engine must not allocate a pool when
  it is off;
- `stop` exits 0.

### Benchmark smoke

```bash
./gradlew :tuff-benchmarks:jmh --rerun --jmhArgs="-wi 3 -i 5 -f 1 -to 120 SubmitPathBenchmark"
```

Compare the shape against [Benchmarks](compute/benchmarks.md), not the absolute rate: that suite ran on
one throttled laptop, and only rows inside a single run are comparable. What you are looking for is a
result that is not an order of magnitude off.

Tick behaviour comes from Folia, so use `/tps` for regions and `/tuff` for the pool. If ticks drop,
stop reading numbers and go to [Rolling back](#rolling-back).

## Compatibility window

Upstream changes that reach us in three different ways.

**Java version.** The toolchain is pinned in the root `build.gradle.kts` (`languageVersion = 25`,
`options.release = 25`) and the workflows use `java-version: 25`. If Folia raises the baseline, change
those, the README sentence about the toolchain, and nothing else. A stale toolchain fails the build
immediately, so this one is loud.

**Paper server API and config.** `CraftServer`, `PaperCommands` and `MinecraftServer` are the files our
patches touch. A moved constructor body, a renamed command map or a reordered shutdown path shows up as
`patch does not apply` at step 4 of the routine. Re-author in the applied sources and re-read the
ordering table above; do not chase it in the `.patch` file.

**Region and chunk-system internals.** `TickRegions`, `TickRegionScheduler`, `MoonriseCommon` are read
by the bootstrap; `folia-supported`, the `folia:*` ticket ids, `io.papermc.paper.threadedregions.*` and
the thread names are Folia's and must stay untouched. When an update touches them:

- compile errors point at `tuff-server/src/main/java/kz/bejiihiu/tuff/**` — fix the call there, that code
  is not a patch and does not need a rebuild;
- a runtime WARN from `Tuff/Compute` about thread counts means the pool keeps its startup size. The
  server is still correct, the pool is just conservative: fix the source read, not the config;
- virtual threads, the queue and the lanes are plain Java in `tuff-compute` and are not affected by a
  Paper or Folia update.

## A new release line (ver/26.4.x)

A Minecraft bump is not a routine merge. When Folia opens a new `ver/*.x` branch:

```bash
git fetch upstream
git switch -c ver/26.4.x upstream/ver/26.4.x
git merge ver/26.3.x
```

The merge base is the old upstream tip, so the merge carries our commits and nothing else, applied by
three-way merge to the new base. Then work through the same conflict list, `cleanCache applyAllPatches`,
rebuild, and update the version references:

- `gradle.properties` — `mcVersion`, `apiVersion` (Folia's values, normally conflict-free);
- `.github/workflows/build.yml` — the `branches-ignore` entry;
- `README.md` — badge branch and the coordinates in the API example;
- `docs/compute/plugin-api.md` — the coordinates;
- release tags follow `v<mcVersion>.build.<N>` (for example `v26.4.build.1`), which `release.yml`
  parses back into the Gradle version.

## Common traps

| Symptom | Cause | What to do |
| --- | --- | --- |
| `patch does not apply` | upstream moved the code the patch was written against | fix the applied sources, commit, rebuild the layer, apply from zero |
| checkstyle fails on `LineEnding` | `core.autocrlf=true` produced CRLF in generated sources | set it to `false`, then re-checkout the generated trees (below) |
| every patch shows as modified after `rb.sh` | a `.patch` was edited by hand, or commits landed on the wrong base | discard the patch changes, work in the applied sources only |
| `applyAllPatches` fails on a dirty tree | leftovers from a previous run | inspect `git -C <applied> status`, then `./gradlew cleanCache applyAllPatches` |
| a Folia fix is missing from the build | its patch file landed in `folia-server/...` after the merge | `git ls-files 'folia-*'`, move the file into `tuff-server/...` |
| two patches share a number | Folia took ours | rename ours to the next number, rebuild |
| new upstream workflow resurrects `release-26.3.yaml` | upstream modified a file we deleted | keep it deleted; our release job is `release.yml` |
| tests pass but the server has no `/tuff` | command registration hook moved during the merge | check `PaperCommands`, re-run the smoke test |

Recovering from bad line endings without touching history:

```bash
for d in paper-server paper-api paper-checkstyle tuff-server/src/minecraft/java; do
  git -C "$d" config core.autocrlf false
  git -C "$d" rm -r -q --cached .
  git -C "$d" reset --hard -q
done
```

## Rolling back

A merge that is not pushed is a `git merge --abort` (during the merge) or `git reset --hard <previous
commit>`. Once it is on the branch, do not force-push: `git revert -m 1 <merge-commit>` records the
rollback as a normal commit, and the branch stays usable for everyone.

Then re-establish the baseline before you investigate:

```bash
./gradlew cleanCache applyAllPatches
./gradlew build
./gradlew :tuff-server:createPaperclipJar
```

If tick times or latency got worse after an update:

1. **Is it us at all?** Run the same jar with `async-compute.enabled: false`. Our patches stay, the
   hooks turn into no-ops. Ticks still bad → the regression is upstream or the merge.
2. **If disabled fixes it**, the engine or its hooks are implicated. Set `workers.threads` to a small
   fixed value (`2`) and compare: unchanged with a tiny pool means the hooks or the budget call, not the
   pool size.
3. **Compare ticks and latency** with `/tps` and `/tuff` (queue wait, executed tasks, rejections). A pool
   that is too large shows up as `activeWorkers` at the limit with the queue empty and CPU high, not as
   a queue.
4. **Compare benchmarks** with the same command on both commits, on the same machine, within one run.
   Never compare an absolute rate from today with a number from another session; see
   [Benchmarks § Limitations](compute/benchmarks.md#limitations).
5. **If it is upstream**, bisect the upstream range in a scratch worktree. Every step needs
   `cleanCache applyAllPatches` plus a tick run, so budget hours, not minutes. Upstream tick regressions
   are reported to Folia, not fixed here.

Keep the failing jar and its `config/tuff.yml` next to the report. The interesting fields are the
`enabled:` line, the `processor budget ... (... region, ... chunk, ... IO)` line and the `lanes:` line
from `tuff`.

## Merge checklist

- [ ] `git ls-files 'folia-*'` is empty; no stray `folia-*` directories
- [ ] our patches are the last ones in `tuff-server/paper-patches/features/` and
      `tuff-server/minecraft-patches/features/`
- [ ] merge conflicts resolved in favour of Tuff's modules, group, publishing and workflows
- [ ] `upstream` `paperRef`, `mcVersion`, `apiVersion` came through the merge unchanged from Folia
- [ ] `./gradlew cleanCache applyAllPatches` succeeds from zero
- [ ] `git status` after `./rb.sh` shows only patches you meant to change
- [ ] `./gradlew build` green, compute test results non-empty
- [ ] paperclip jar exists; brand manifest and engine classes verified
- [ ] smoke run: engine enabled, `/tuff`, clean stop, exit 0
- [ ] smoke run: `async-compute.enabled: false`, no `Tuff Compute` threads
- [ ] benchmark smoke not an order of magnitude off
- [ ] CI `build` green on the branch (`gh workflow run build.yml --ref ver/26.3.x -R bejiihiu/tuff`)
