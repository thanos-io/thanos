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

# 2. Add headers to special CODE_OF_CONDUCT.md, CONTRIBUTING.md and CHANGELOG.md files.
echo "$(cat <<EOT
---
title: Code of Conduct
type: docs
menu: contributing
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md
tail -n +2 CODE_OF_CONDUCT.md >> ${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md

echo "$(cat <<EOT
---
title: Contributing
type: docs
menu: contributing
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md
tail -n +2 CONTRIBUTING.md >> ${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md

echo "$(cat <<EOT
---
title: Changelog
type: docs
menu: thanos
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CHANGELOG.md
tail -n +2 CHANGELOG.md >> ${OUTPUT_CONTENT_DIR}/CHANGELOG.md

ALL_DOC_CONTENT_FILES=`echo "${OUTPUT_CONTENT_DIR}/**/*.md ${OUTPUT_CONTENT_DIR}/*.md"`
for file in ${ALL_DOC_CONTENT_FILES}
do

  relFile=${file#*/*/}
  echo "$(cat <<EOT

---

Found a typo, inconsistency or missing information in our docs?
Help us to improve [Thanos](https://thanos.io) documentation by proposing a fix [on GitHub here](https://github.com/improbable-eng/thanos/edit/master/docs/${relFile}) :heart:

EOT
)" >> ${file}

done

# 3. All the absolute links needs are directly linking github with the given commit.
perl -pi -e 's/]\(\//]\(https:\/\/github.com\/improbable-eng\/thanos\/tree\/'${COMMIT_SHA}'\/docs\//' ${ALL_DOC_CONTENT_FILES}

# 4. All the relative links needs to have ../  This is because Hugo is missing: https://github.com/gohugoio/hugo/pull/3934
perl -pi -e 's/]\(\.\//]\(..\//' ${ALL_DOC_CONTENT_FILES}
perl -pi -e 's/]\((?!http)/]\(..\//' ${ALL_DOC_CONTENT_FILES}
perl -pi -e 's/src=\"(?!http)/src=\"..\//' ${ALL_DOC_CONTENT_FILES}