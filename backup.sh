#!/usr/bin/env bash
set -euo pipefail
git add -A
if git diff --cached --quiet; then
  echo "No changes to backup."
  exit 0
fi
MSG="${1:-backup $(date '+%Y-%m-%d %H:%M:%S')}"
git commit -m "$MSG"
git push
echo "Backup pushed."
