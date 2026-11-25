#!/usr/bin/env bash
# rebuild all patch sets from the applied sources
set -euo pipefail
./gradlew rebuildPaperApiPatches rebuildPaperCheckstylePatches rebuildPaperCheckstyleConfigPatches :tuff-server:rebuildAllServerPatches
