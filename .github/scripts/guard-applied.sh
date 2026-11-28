#!/usr/bin/env bash
# Post-apply guards. applyAllPatches happily drops a hook into a dead spot and the
# build still compiles, and a path regex can silently stop matching a renamed module.
# Both classes of mess already happened here once (checkstyle after the tuff rename).
set -euo pipefail

CRAFT_SERVER="paper-server/src/main/java/org/bukkit/craftbukkit/CraftServer.java"
PAPER_COMMANDS="paper-server/src/main/java/io/papermc/paper/command/PaperCommands.java"
MINECRAFT_SERVER="tuff-server/src/minecraft/java/net/minecraft/server/MinecraftServer.java"
TICK_SCHEDULER="tuff-server/src/minecraft/java/io/papermc/paper/threadedregions/TickRegionScheduler.java"
CHECKSTYLE="paper-server/.checkstyle/checkstyle.xml"

fail() {
  echo "::error::$*" >&2
  exit 1
}

require_file() {
  [ -f "$1" ] || fail "$1 is missing - applyAllPatches did not lay out the applied tree (or the path moved upstream)."
}

count_of() {
  local n
  n=$(grep -cF -e "$1" "$2" || true)
  printf '%s' "${n:-0}"
}

expect_once() {
  local pattern="$1" file="$2" what="$3" hits
  hits=$(count_of "$pattern" "$file")
  [ "$hits" -eq 1 ] || fail "$what: expected exactly one '$pattern' in $file, found $hits. Re-anchor it in the applied sources and rebuild the patch - never edit the .patch by hand."
}

line_of() {
  local n
  n=$(grep -nF -m1 -e "$1" "$2" | cut -d: -f1 || true)
  printf '%s' "${n:-0}"
}

# the needle must sit strictly between two anchors, because "compiles" != "runs at the right moment"
expect_between() {
  local label="$1" needle="$2" before="$3" after="$4" file="$5"
  local at lo hi
  at=$(line_of "$needle" "$file")
  lo=$(line_of "$before" "$file")
  hi=$(line_of "$after" "$file")
  [ "$at" -gt 0 ] || fail "$label: '$needle' is gone from $file."
  [ "$lo" -gt 0 ] || fail "$label: anchor '$before' is gone from $file - upstream moved it, re-check where the hook belongs."
  [ "$hi" -gt 0 ] || fail "$label: anchor '$after' is gone from $file - upstream moved it, re-check where the hook belongs."
  { [ "$at" -gt "$lo" ] && [ "$at" -lt "$hi" ]; } \
    || fail "$label: '$needle' is at line $at, outside '$before' ($lo) .. '$after' ($hi). It still compiles there, but it runs at the wrong moment."
}

for f in "$CRAFT_SERVER" "$PAPER_COMMANDS" "$MINECRAFT_SERVER" "$TICK_SCHEDULER" "$CHECKSTYLE"; do
  require_file "$f"
done

# 1. CraftServer: the engine starts at the end of the constructor, before plugins load.
expect_once 'ComputeBootstrap.start()' "$CRAFT_SERVER" "compute start hook"
expect_between 'compute start hook' 'ComputeBootstrap.start()' \
  'public CraftServer(' 'public boolean getCommandBlockOverride' "$CRAFT_SERVER"

# 2. MinecraftServer: budget right after the regionized server is up.
expect_once 'ComputeBootstrap.applyRegionBudget()' "$MINECRAFT_SERVER" "region budget hook"
expect_between 'region budget hook' 'ComputeBootstrap.applyRegionBudget()' \
  'RegionizedServer.getInstance().init()' 'final long actualDoneTimeMs' "$MINECRAFT_SERVER"

# 3. MinecraftServer: stop after async tasks drained, before the world save/connection teardown.
expect_once 'ComputeBootstrap.stop()' "$MINECRAFT_SERVER" "compute stop hook"
expect_between 'compute stop hook' 'ComputeBootstrap.stop()' \
  'waitForAsyncTasksShutdown()' 'this.getConnection().stop()' "$MINECRAFT_SERVER"

# 4. PaperCommands: /tuff is registered in the COMMANDS map (fully qualified, no imports).
expect_once 'COMMANDS.put("tuff"' "$PAPER_COMMANDS" "tuff command"
expect_between 'tuff command' 'COMMANDS.put("tuff"' \
  'COMMANDS.put("paper"' 'COMMANDS.forEach' "$PAPER_COMMANDS"

# 5. Rename invariant. tuff-server/src/minecraft is not "paper-server" anymore, and
# before 4d4253a the regex still said (paper|folia) - checkstyle then linted the
# decompiled NMS tree and the build died on code we do not own.
pattern='value="[\\/](paper|folia|tuff)-server[\\/]src[\\/]minecraft[\\/]"'
hits=$(count_of "$pattern" "$CHECKSTYLE")
[ "$hits" -eq 1 ] || fail "checkstyle exclusion for tuff-server/src/minecraft is wrong in $CHECKSTYLE (found $hits). The fileNamePattern must keep the tuff alternative: (paper|folia|tuff)-server/src/minecraft - anything narrower starts linting decompiled NMS (4d4253a)."

# 6. 0016 seams. All three compile wherever they sit, so each one is pinned to its
# place: the observer call fires from runTick's finally once the tick bookkeeping is
# done, the catch up limit is read and consumed before the new tick count is used, and
# the region thread factory wraps the body it hands to TickThreadRunner.
expect_once 'TickObservers.observer()' "$TICK_SCHEDULER" "tick observer call"
expect_between 'tick observer call' 'TickObservers.observer()' \
  'TickRegionScheduler.setTickingRegion(null);' '// Only AFTER updating the tickStart' "$TICK_SCHEDULER"

expect_once 'final long writeOff = this.writeOffLimit;' "$TICK_SCHEDULER" "catch up limit read"
expect_between 'catch up limit read' 'final long writeOff = this.writeOffLimit;' \
  'final long periodsAhead' 'final long tickCount = Math.max' "$TICK_SCHEDULER"

expect_once 'this.writeOffLimit = 0L;' "$TICK_SCHEDULER" "catch up limit clear"

expect_once 'RegionThreads.wrapBody(run)' "$TICK_SCHEDULER" "region bridge wrap"
expect_between 'region bridge wrap' 'RegionThreads.wrapBody(run)' \
  'public Thread newThread(' 'ret.setUncaughtExceptionHandler' "$TICK_SCHEDULER"

echo "applied-tree guards ok: 4 hooks in place, checkstyle covers tuff-server/src/minecraft, 0016 seams in place"
