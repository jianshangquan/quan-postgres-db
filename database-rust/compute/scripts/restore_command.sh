#!/bin/bash
# Placeholder restore_command for restart-from-storage demos.
# Full Neon-style restore pulls WAL from safekeeper/S3; v1 no-ops.
set -euo pipefail
echo "restore_command: not implemented for segment $2 (v1 placeholder)" >&2
exit 1
