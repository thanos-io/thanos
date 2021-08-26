#!/usr/bin/env bash

OUTPUT_CONTENT_DIR=$1
TOP_WEIGHT=$2
COMMIT_SHA=$(git rev-parse HEAD)

echo ">> postprocessing content of dir ${OUTPUT_CONTENT_DIR}"

# Create an _index.md in this dir to enable sorting capabilities and make this version appear top in version picker
echo "$(
  cat <<EOF
---
weight: ${TOP_WEIGHT}
---
EOF
)" >${OUTPUT_CONTENT_DIR}/_index.md

# Create a thanos/_index.md file to make sure links work.
echo "$(
  cat <<EOF
---
title: 'Thanos General Documents:'
---
EOF
)" >${OUTPUT_CONTENT_DIR}/thanos/_index.md
