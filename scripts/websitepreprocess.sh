#!/usr/bin/env bash

if ! [[ "$0" =~ "scripts/websitepreprocess.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

WEBSITE_DIR="website"
ORIGINAL_CONTENT_DIR="docs"
OUTPUT_CONTENT_DIR="${WEBSITE_DIR}/docs-pre-processed"
COMMIT_SHA=`git rev-parse HEAD`

rm -rf ${OUTPUT_CONTENT_DIR}
mkdir -p ${OUTPUT_CONTENT_DIR}

# 1. Copy original content.
cp -r ${ORIGINAL_CONTENT_DIR}/* ${OUTPUT_CONTENT_DIR}

# 2. Copy extra files to content: CODE_OF_CONDUCT.md, CONTRIBUTING.md and CHANGELOG.md files.
echo "$(cat <<EOT
---
title: Code of Conduct
type: docs
menu: contributing
---

EOT
)$(cat CODE_OF_CONDUCT.md)" > ${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md

echo "$(cat <<EOT
---
title: Contributing
type: docs
menu: contributing
---

EOT
)$(cat CONTRIBUTING.md)" > ${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md

echo "$(cat <<EOT
---
title: Changelog
type: docs
menu: thanos
---

EOT
)$(cat CHANGELOG.md)" > ${OUTPUT_CONTENT_DIR}/CHANGELOG.md

# 3. All the absolute links needs are directly linking github with the given commit.
perl -pi.bak -e 's/]\(\//]\(https:\/\/github.com\/improbable-eng\/thanos\/tree\/'${COMMIT_SHA}'\/docs\//' ${OUTPUT_CONTENT_DIR}/*.md ${OUTPUT_CONTENT_DIR}/**/*.md

# 4. All the relative links needs to have ../  This is because Hugo is missing: https://github.com/gohugoio/hugo/pull/3934
perl -pi.bak -e 's/]\(\.\//]\(..\//' ${OUTPUT_CONTENT_DIR}/*.md ${OUTPUT_CONTENT_DIR}/**/*.md
perl -pi.bak -e 's/]\((?!http)/]\(..\//' ${OUTPUT_CONTENT_DIR}/*.md ${OUTPUT_CONTENT_DIR}/**/*.md
perl -pi.bak -e 's/src=\"(?!http)/src=\"..\//' ${OUTPUT_CONTENT_DIR}/*.md ${OUTPUT_CONTENT_DIR}/**/*.md

# Pass Google analytics token:
sed -e 's/${GOOGLE_ANALYTICS_TOKEN}/'${GOOGLE_ANALYTICS_TOKEN}'/' ${WEBSITE_DIR}/hugo.tmpl.yaml > ${WEBSITE_DIR}/hugo-generated.yaml
