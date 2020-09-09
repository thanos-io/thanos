#!/usr/bin/env bash
#
# Build React web UI.
# Run from repository root.
set -e
set -u

if ! [[ "scripts/build-react-app.sh" =~ $0 ]]; then
  echo "must be run from repository root"
  exit 255
fi

cd pkg/ui/react-app

PUBLIC_URL=. yarn build
rm -rf ../static/react
mv build ../static/react
