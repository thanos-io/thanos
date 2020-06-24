#!/usr/bin/env bash

# if ! [[ "$0" =~ "scripts/docsversioning.sh" ]]; then
# 	echo "must be run from repository root"
# 	exit 255
# fi

WEBSITE_DIR="website"
GITHUB_REPO="https://github.com/thanos-io/thanos"
ORIGINAL_CONTENT_DIR="docs"
OUTPUT_CONTENT_DIR="${WEBSITE_DIR}/docs-pre-processed/versioned"
COMMIT_SHA=`git rev-parse HEAD`

mkdir -p ${OUTPUT_CONTENT_DIR}

# cd ${OUTPUT_CONTENT_DIR}

# Clone docs from release branch.
# for branch in $(git branch --all | grep '^\s*remotes' | grep 'release-\.*' | egrep --invert-match '(:?HEAD|master)$'); do
#      git clone "${GITHUB_REPO}" 
# done

# Clone all upstream 'release' branch to origin.
for branch in $(git branch --all | grep '^\s*remotes' | grep 'release-.*'| egrep --invert-match '(:?HEAD|master)$'); do
    git branch --track "${branch##*/}" "$branch"
done

# Clone docs from release branch.
git for-each-ref --format='%(refname)' refs/heads/ | grep 'release-.*' | head -n 3 | while read branchRef; do
    echo ">> cloning docs for versioning"
    branchName=${branchRef#refs/heads/}
    mkdir -p ${OUTPUT_CONTENT_DIR}/"$branchName"
git archive --format=tar "$branchRef" | tar -C${OUTPUT_CONTENT_DIR}/$branchName -x "docs/" --strip-components=1

done

# Sort the list of release branches and choose the latest. Copy it into latest folder.
LATEST=$(ls ${OUTPUT_CONTENT_DIR} | sort -nr | head -n1)
cp -a ${OUTPUT_CONTENT_DIR}/$LATEST ${OUTPUT_CONTENT_DIR}/latest

# for file in ${OUTPUT_CONTENT_DIR} do
# LATEST=$(ls 'release-.*' | sort -nr | head -n1)
# echo scp -rf /docs/$LATEST ${OUTPUT_CONTENT_DIR}/latest
# done