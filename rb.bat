@echo off
REM rebuild all patch sets from the applied sources, see notes/upstream.md
gradlew rebuildPaperApiPatches rebuildPaperCheckstylePatches rebuildPaperCheckstyleConfigPatches :tuff-server:rebuildAllServerPatches
