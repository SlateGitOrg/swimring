#!/bin/sh
# Usage: coverage.sh <coverprofile> <minimum-percent>
# Prints the total statement coverage of the profile and fails if it is
# below the minimum.
set -eu
profile="$1"
min="$2"
total=$(go tool cover -func="$profile" | awk '/^total:/ { sub("%", "", $3); print $3 }')
echo "core coverage (packages: root, clock, transport, sim, swim, ring, cache, experiment): ${total}%"
awk -v t="$total" -v m="$min" 'BEGIN { if (t + 0 < m + 0) { printf "coverage %.1f%% is below the %s%% gate\n", t, m; exit 1 } }'
echo "coverage gate (>= ${min}%): ok"
