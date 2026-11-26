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

echo "static patch guards ok: no stray folia-*, 4 hooks, checkstyle + gitignore invariants"
