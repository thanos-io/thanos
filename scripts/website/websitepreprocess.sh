#!/usr/bin/env bash

# Regexp take from https://semver.org/
# If we want to limit those we can sort, and have only head -n X of them etc
RELEASE_FILTER_RE="release-(0|[1-9]\d*)\.(0|[1-9]\d*)$"
WEBSITE_DIR="website"
ORIGINAL_CONTENT_DIR="docs"
OUTPUT_CONTENT_DIR="${WEBSITE_DIR}/docs-pre-processed"
FILES="${WEBSITE_DIR}/docs-pre-processed/*"

git remote add upstream https://github.com/thanos-io/thanos.git
git remote add origin https://github.com/thanos-io/thanos.git
git remote -v
git fetch origin

RELEASE_BRANCHES=$(git branch --all | grep -P "remotes/origin/${RELEASE_FILTER_RE}" | egrep --invert-match '(:?HEAD|main)$' | sort -V)
echo ">> chosen $(echo ${RELEASE_BRANCHES}) releases to deploy docs from"

rm -rf ${OUTPUT_CONTENT_DIR}
mkdir -p "${OUTPUT_CONTENT_DIR}/tip"

# Copy original content from current state first.
cp -r ${ORIGINAL_CONTENT_DIR}/* "${OUTPUT_CONTENT_DIR}/tip"
scripts/website/contentpreprocess.sh "${OUTPUT_CONTENT_DIR}/tip" 100000

#create variable for weight value
WEIGHT_VALUE=0

for branchRef in ${RELEASE_BRANCHES}; do
  WEIGHT_VALUE=$((WEIGHT_VALUE + 1))
  branchName=${branchRef##*/}
  tags=${branchName/release-/v}
  echo ">> cloning docs for versioning ${tags}"
  mkdir -p "${OUTPUT_CONTENT_DIR}/${tags}"
  git archive --format=tar "refs/${branchRef}" | tar -C${OUTPUT_CONTENT_DIR}/${tags} -x "docs/" --strip-components=1
  scripts/website/contentpreprocess.sh "${OUTPUT_CONTENT_DIR}/${tags}" ${WEIGHT_VALUE}
done
