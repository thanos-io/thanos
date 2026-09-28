#!/usr/bin/env bash
set -euo pipefail

dups=$(grep -rhoE 'e2e\.NewDockerEnvironment\("[^"]+"' --include='*.go' . | sort | uniq -d)
if [[ -n $dups ]]; then
  echo "duplicate docker environment names:"
  grep -rnF --include='*.go' -f <(echo "$dups") .
  exit 1
fi

grep -rnoE 'e2e\.NewDockerEnvironment\([^)"]+\)' --include='*.go' . &&
  echo "^ non-literal names above, check uniqueness manually"

echo "OK: all literal environment names are unique"
