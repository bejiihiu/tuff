@echo off
REM rebuild all patch sets from the applied sources
gradlew rebuildPaperApiPatches rebuildPaperCheckstylePatches rebuildPaperCheckstyleConfigPatches :tuff-server:rebuildAllServerPatches
