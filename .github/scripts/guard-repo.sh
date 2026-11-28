#!/usr/bin/env bash
# Cheap static guards for a direct push to ver/26.3.x. build.yml ignores that branch
# on purpose (a full apply + build is ~20 min), so this checks the committed patch
# invariants in a couple of seconds instead. Needs no gradle and no network.
set -euo pipefail

CHECKSTYLE_PATCH="tuff-server/paper-patches/files/.checkstyle/checkstyle.xml.patch"

fail() {
  echo "::error::$*" >&2
  exit 1
}

# A Folia file that landed under the old folia-* name silently never applies.
stray=$(git ls-files -- 'folia-*')
[ -z "$stray" ] || fail "stray upstream paths: ${stray//$'\n'/ }. Move them into the matching tuff-* directory."

[ -f "$CHECKSTYLE_PATCH" ] || fail "$CHECKSTYLE_PATCH is gone - the checkstyle exclusion for the decompiled NMS tree is missing."

grep -qF -e 'value="[\\/](paper|folia|tuff)-server[\\/]src[\\/]minecraft[\\/]"' "$CHECKSTYLE_PATCH" \
  || fail "$CHECKSTYLE_PATCH no longer excludes tuff-server/src/minecraft (the 4d4253a incident). The fileNamePattern must keep the tuff alternative: (paper|folia|tuff)-server/src/minecraft."

grep -qxF '/tuff-server/src/minecraft' .gitignore \
  || fail ".gitignore must keep ignoring /tuff-server/src/minecraft - otherwise applyAllPatches can leak the decompiled NMS tree into a commit."

expect_one_in_patches() {
  local pattern="$1" what="$2" hits files
  hits=$(grep -rF -h -e "$pattern" tuff-server/paper-patches tuff-server/minecraft-patches 2>/dev/null | wc -l | tr -d ' ')
  files=$(grep -rF -l -e "$pattern" tuff-server/paper-patches tuff-server/minecraft-patches 2>/dev/null || true)
  [ "$hits" -eq 1 ] || fail "$what: expected '$pattern' in exactly one patch, found $hits. Files: ${files:-none}"
}

expect_one_in_patches 'ComputeBootstrap.start()' 'compute start hook'
expect_one_in_patches 'ComputeBootstrap.applyRegionBudget()' 'region budget hook'
expect_one_in_patches 'ComputeBootstrap.stop()' 'compute stop hook'
expect_one_in_patches 'COMMANDS.put("tuff"' 'tuff command'

# Our patches sit on top of the upstream stack. Upstream keeps appending numbered
# feature patches, so "ours is last" is asserted here instead of assumed: a patch
# renumbered below an upstream one silently interleaves our hunks with theirs.
features_in() {
  ls "$1" | grep -E '^[0-9]{4}-.*\.patch$' | sort || true
}

expect_last_patch() {
  local dir="$1" number="$2" last
  last=$(features_in "$dir" | tail -n 1)
  case "$last" in
    "$number"-*) ;;
    *) fail "$dir: the highest numbered patch is '${last:-none}', expected $number-*. Ours always go after the upstream set - renumber instead of inserting below it." ;;
  esac
}

PAPER_FEATURES="tuff-server/paper-patches/features"
MINECRAFT_FEATURES="tuff-server/minecraft-patches/features"

expect_last_patch "$PAPER_FEATURES" 0009
expect_last_patch "$MINECRAFT_FEATURES" 0016

# 0015 (lifecycle) is the patch right under 0016 (region hooks); an upstream patch
# landing between them would split one change set across two numbers.
second_last=$(features_in "$MINECRAFT_FEATURES" | tail -n 2 | head -n 1)
case "$second_last" in
  0015-*) ;;
  *) fail "$MINECRAFT_FEATURES: expected 0015-* directly under 0016, found '${second_last:-none}'." ;;
esac

# The seams 0016 owns are load bearing: the tick observer call is the only source of
# per-body lag, the catch up limit is a bounded write off that must be consumed, and
# the region bridge wrap is what makes a tick body visible to the thread layer.
PATCH_0016="tuff-server/minecraft-patches/features/0016-Tuff-hook-region-ticks-for-accounting-and-Kairos.patch"
[ -f "$PATCH_0016" ] || fail "$PATCH_0016 is gone - the region hook patch was renamed or merged into another number."

added_0016=$(grep -E '^\+' "$PATCH_0016" || true)

expect_added_0016() {
  local needle="$1" what="$2"
  printf '%s\n' "$added_0016" | grep -qF -- "$needle" \
    || fail "0016 no longer adds the $what seam ('$needle'). The applied guard checks it again, but losing it here means the patch was rebuilt wrong."
}

expect_added_0016 'TickObservers.observer()' 'tick observer call'
expect_added_0016 'final long writeOff = this.writeOffLimit;' 'catch up limit read'
expect_added_0016 'this.writeOffLimit = 0L;' 'catch up limit clear'
expect_added_0016 'RegionThreads.wrapBody(run)' 'region bridge wrap'

echo "static patch guards ok: no stray folia-*, 4 hooks, checkstyle + gitignore invariants, our patches last (paper 0009, minecraft 0015/0016) with all 0016 seams"
