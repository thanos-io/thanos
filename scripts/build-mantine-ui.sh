#!/usr/bin/env bash
#
# Build React web UI.
# Run from repository root.
set -e
set -u

if ! [[ "scripts/build-mantine-ui.sh" =~ $0 ]]; then
  echo "must be run from repository root"
  exit 255
fi

cd pkg/ui/mantine-ui

npm run build
rm -rf ../static/mantine-ui
mkdir -p ../static
mv dist ../static/mantine-ui
