#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export SAFEKEEPER="${SAFEKEEPER:-127.0.0.1:25051,127.0.0.1:25052,127.0.0.1:25053}"
export PAGESERVER="${PAGESERVER:-127.0.0.1:25061}"
echo "Running smoke against safekeeper=$SAFEKEEPER pageserver=$PAGESERVER"
cargo run -q --bin smoke
echo "SMOKE OK"
