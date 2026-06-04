#!/usr/bin/env bash
set -euo pipefail

EXERCISE="${1:-all}"

mvn -q compile exec:java \
  -Dexec.mainClass=it.polimi.sda.flinkf1.F1FlinkCoursewareJob \
  -Dexec.args="$EXERCISE"
