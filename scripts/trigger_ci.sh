#!/usr/bin/env bash

# Target branch from the contributor's PR we want to build CI against.
# In a form of TARGET_USER:TARGET_BRANCH (as it is in GitHub PR header)
TARGET_USER=${1%%":"*}
TARGET_BRANCH=${1##*":"}

LOCAL_BRANCH_NAME="${TARGET_USER}-${TARGET_BRANCH}-ci-build"

# Try to save your current unstaged changes.
OLDSHA=$(git rev-parse -q --verify refs/stash)
git stash -q
NEWSHA=$(git rev-parse -q --verify refs/stash)
if [ "${OLDSHA}" = "${NEWSHA}" ]; then
    IS_STASHED=false
else
    IS_STASHED=true
fi

git branch -D "${LOCAL_BRANCH_NAME}"
git checkout -b "${LOCAL_BRANCH_NAME}" master
git pull "https://github.com/${TARGET_USER}/thanos.git" "${TARGET_BRANCH}"

git push origin "${LOCAL_BRANCH_NAME}" --force
git checkout -

# Apply back your own changes if needed.
if ${IS_STASHED}; then git stash pop; fi