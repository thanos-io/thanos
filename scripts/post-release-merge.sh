#!/bin/bash
# Usage: ./scripts/post-release-merge.sh release-0.38

set -e

RELEASE_BRANCH="$1"

if [ -z "$RELEASE_BRANCH" ]; then
  echo "Usage: $0 <release-branch>"
  exit 1
fi

echo "Fetching all branches..."
git fetch --all

echo "Checking out main branch..."
git checkout main

echo "Merging $RELEASE_BRANCH into main..."
git merge --no-ff "$RELEASE_BRANCH" -m "Merge $RELEASE_BRANCH into main after release"

echo "Pushing changes to origin..."
git push origin main

echo "✅ Done: $RELEASE_BRANCH merged into main."
