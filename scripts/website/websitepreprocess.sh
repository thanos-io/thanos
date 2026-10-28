#!/usr/bin/env bash

# Regexp take from https://semver.org/
# If we want to limit those we can sort, and have only head -n X of them etc
RELEASE_FILTER_RE="release-(0|[1-9]\d*)\.(0|[1-9]\d*)$"
WEBSITE_DIR="website"
ORIGINAL_CONTENT_DIR="docs"
FILES="${WEBSITE_DIR}/docs-pre-processed/*"
MDOX_CONFIG=".mdox.yaml"
MDOX_PREV_CONFIG=".mdox.prev-release.yaml"

# Support gtar and ggrep on OSX (installed via brew), falling back to tar and grep. On Linux
# systems gtar or ggrep won't be installed, so will use tar and grep as expected.
TAR=$(which gtar 2>/dev/null || which tar)
GREP=$(which ggrep 2>/dev/null || which grep)

# Exported for use in .mdox.prev-release.yaml
export OUTPUT_CONTENT_DIR="${WEBSITE_DIR}/docs-pre-processed"

git remote add upstream https://github.com/thanos-io/thanos.git
git remote add origin https://github.com/thanos-io/thanos.git
git remote -v
git fetch origin

# release-41.0 contains the v0.41 docs. Use v0.41 for URLs and version ordering,
# while keeping the original Git ref to read the docs from that branch.
documentation_version() {
  local branchName=${1##*/}
  if [[ ${branchName} == "release-41.0" ]]; then
    echo "v0.41"
  else
    echo "${branchName/release-/v}"
  fi
}

RELEASE_BRANCHES=$(
  git branch --all | $GREP -P "remotes/origin/${RELEASE_FILTER_RE}" | egrep --invert-match '(:?HEAD|main)$' |
    while read -r branchRef; do
      printf '%s\t%s\n' "$(documentation_version "${branchRef}")" "${branchRef}"
    done | sort -k1,1V | cut -f2
)
echo ">> chosen $(echo ${RELEASE_BRANCHES}) releases to deploy docs from"

# preprocess tip separately
rm -rf ${OUTPUT_CONTENT_DIR}
PATH=$PATH:$GOBIN
# Exported for use in .mdox.yaml.
export INPUT_DIR="docs"
export OUTPUT_DIR="${OUTPUT_CONTENT_DIR}/tip"
export EXTERNAL_GLOB_REL="../"
$MDOX transform --log.level=debug --config-file=$MDOX_CONFIG
scripts/website/mdoxpostprocess.sh "${OUTPUT_CONTENT_DIR}/tip" 100000

#create variable for weight value
WEIGHT_VALUE=0

for branchRef in ${RELEASE_BRANCHES}; do
  WEIGHT_VALUE=$((WEIGHT_VALUE + 1))
  # Exported for use in .mdox.prev-release.yaml
  export tags=$(documentation_version "${branchRef}")
  echo ">> cloning docs for versioning ${tags}"
  mkdir -p "${OUTPUT_CONTENT_DIR}/${tags}-git-docs"
  git archive --format=tar "refs/${branchRef}" | $TAR -C${OUTPUT_CONTENT_DIR}/${tags}-git-docs -x "docs/" --strip-components=1
  # Frontmatter isn't present after v0.22 so .mdox.yaml is used after that.
  if [[ $WEIGHT_VALUE -gt 22 ]]; then
    # Exported for use in .mdox.yaml.
    export INPUT_DIR="${OUTPUT_CONTENT_DIR}/${tags}-git-docs"
    export OUTPUT_DIR="${OUTPUT_CONTENT_DIR}/${tags}"
    export EXTERNAL_GLOB_REL="../../../"
    $MDOX transform --log.level=debug --config-file=$MDOX_CONFIG
  else
    $MDOX transform --log.level=debug --config-file=$MDOX_PREV_CONFIG
  fi
  scripts/website/mdoxpostprocess.sh "${OUTPUT_CONTENT_DIR}/${tags}" ${WEIGHT_VALUE}
  rm -rf ${OUTPUT_CONTENT_DIR}/${tags}-git-docs
done
