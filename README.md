# Tuff

[![Build](https://img.shields.io/github/actions/workflow/status/bejiihiu/tuff/build.yml?branch=ver/26.3.x&label=build)](https://github.com/bejiihiu/tuff/actions)
[![License: GPL v3](https://img.shields.io/badge/license-GPLv3-blue.svg)](./PATCHES-LICENSE)

<div align=center>
    <p><b>Tuff</b> is a fork of <a href="https://github.com/PaperMC/Folia">Folia</a>, which itself is a fork of
    <a href="https://github.com/PaperMC/Paper">Paper</a>. Folia adds regionised multithreading to the dedicated server; Tuff keeps
    all of that machinery untouched and builds its own stuff on top of it.</p>
</div>

## What Tuff changes

Tuff is a **branding + feature** layer over Folia. Upstream Folia (and therefore Paper) is merged regularly, so everything
inside `io.papermc.paper.threadedregions.*`, the `folia-supported` plugin flag, the `folia:*` ticket ids and the rest of the
Folia internals are exactly the same as upstream. No renames, no "hidden" Folia.

On top of that, Tuff ships:

- its own server identity (`Brand-Name: Tuff`, `Brand-Id: kz.bejiihiu:tuff`, `tuff-bundler` / `tuff-paperclip` jars);
- an update checker that talks to [our GitHub releases](https://github.com/bejiihiu/tuff/releases) instead of PaperMC's fill API;
- bStats submissions disabled by default (Folia's page is not ours). Opt in with `-Dtuff.bstats=true` if you really want them;
- a home for new systems in the `kz.bejiihiu.tuff` package. That code lives in `tuff-server/src/main/java`, outside of the patch stack,
  so it never gets in the way of upstream updates.

## Building

You need Git and a JDK. Gradle picks up JDK 25 through the toolchain resolver, so a local JDK 21 install is fine.

```bash
git clone https://github.com/bejiihiu/tuff.git
cd tuff
./gradlew applyAllPatches   # decompiles Minecraft and applies the patch stack
./gradlew build             # produces tuff-server/build/libs/tuff-paperclip-<version>.jar
```

Windows: use `gradlew.bat` (or the helper scripts `patch.bat` / `rb.bat`). Make sure `git config --global core.autocrlf false`
is set before the first build, otherwise checkstyle fails on CRLF in the generated sources.

Downloads for people who just want to run a server are on the
[releases page](https://github.com/bejiihiu/tuff/releases).

## Using the API

`tuff-api` is published to GitHub Packages:

```kotlin
repositories {
    maven("https://maven.pkg.github.com/bejiihiu/tuff") {
        credentials {
            username = "your-github-name"
            password = System.getenv("GITHUB_TOKEN") // needs read:packages
        }
    }
}

dependencies {
    compileOnly("kz.bejiihiu.tuff:tuff-api:26.3.local-SNAPSHOT")
}
```

For local plugin development the simplest option is:

```bash
./gradlew publishToMavenLocal
```

and then use `mavenLocal()` with the same coordinates. Publishing from CI uses the `tuffUsername` / `tuffPassword`
Gradle properties (see `.github/workflows/release.yml`).

## Tuff compute

Tuff has a shared compute scheduler for CPU-bound work: priorities with aging, per-consumer quotas and
fair-share weights, a bounded queue, an optional virtual-thread lane for blocking work, metrics and JFR.
Plugins reach it through `TuffCompute` and hand results back to the owning region with `ComputeHandoff`; the
configuration lives in `config/tuff.yml`. Built-in consumers are not part of this build yet; `/tuff` shows
the engine state and controls its JFR recording. The chunk system keeps its own ordered-stream executors —
see [why](docs/compute/chunk-system.md). Start with [`docs/compute/`](docs/compute/README.md) and the
server presets in [`tuning.md`](docs/compute/tuning.md).

## Region threading (the Folia part)

Folia groups nearby loaded chunks into an "independent region". See
[the PaperMC documentation](https://docs.papermc.io/folia/reference/region-logic) for the exact grouping rules.
Each independent region has its own tick loop, ticked at the regular Minecraft tickrate (20 TPS). The tick loops run on a
thread pool in parallel. There is no main thread anymore: each region effectively has its own "main thread" that executes
the entire tick loop.

For a server with many spread out players, this creates many spread out regions and ticks them all in parallel on a
configurable sized threadpool, so it scales well for skyblock/SMP style servers with a sizeable player count. Ideally you
want at least 16 _cores_ (not threads).

### Configuration starting point

Pre-generate your world first, then allocate threads roughly like this (based on a ~330 player peak test, tune from there):

- netty IO: ~4 per 200-300 players
- chunk system io threads: ~3 per 200-300 players
- chunk system workers (pre-generated): ~2 per 200-300 players
- leave ~20% of the cores free for GC and anything plugins spawn
- the rest goes to tick threads (`threaded-regions.threads` in the global config)

## Plugin compatibility

There is no main thread. Expect every plugin to need at least some changes to run on Folia-style threading, and plugins that
share mutable state between regions will race. Only plugins explicitly marked with `folia-supported: true` in their
`plugin.yml` are loaded at all. That flag is unchanged in Tuff, so existing Folia plugins work as-is.

Folia's own API additions (unchanged in Tuff):

- `RegionScheduler`, `AsyncScheduler`, `GlobalRegionScheduler` and `EntityScheduler` replace `BukkitScheduler`.
- `Bukkit#isOwnedByCurrentRegion` tests whether the current region owns a position or entity.
- `Entity#getScheduler` follows the entity across teleports.

Known broken upstream API (same as Folia): portals/respawn/login bits, all scoreboard API, world loading/unloading,
and `Entity#teleport` (use `teleportAsync`).

## Credits and license

Tuff would not exist without [Paper](https://github.com/PaperMC/Paper) and [Folia](https://github.com/PaperMC/Folia).
Their code, patch stack and documentation are reused under their licenses; all upstream copyright headers are kept intact.

The patch stack (`tuff-api/paper-patches`, `tuff-server/paper-patches`, `tuff-server/minecraft-patches`) is licensed under the
GNU General Public License v3, see [`PATCHES-LICENSE`](./PATCHES-LICENSE). If you distribute a Tuff build, you must also make
the corresponding source available.

Paper, Folia and their logos are property of PaperMC. Tuff is not affiliated with or endorsed by PaperMC.
