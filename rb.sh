#!/usr/bin/env bash
# rebuild all patch sets from the applied sources, see notes/upstream.md
set -euo pipefail
./gradlew rebuildPaperApiPatches rebuildPaperCheckstylePatches rebuildPaperCheckstyleConfigPatches :tuff-server:rebuildAllServerPatches
