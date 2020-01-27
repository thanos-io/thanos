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

# Copy original content.
cp -r ${ORIGINAL_CONTENT_DIR}/* ${OUTPUT_CONTENT_DIR}

# Add edit footer to `docs/` md items.
ALL_DOC_CONTENT_FILES=`echo "${OUTPUT_CONTENT_DIR}/**/*.md ${OUTPUT_CONTENT_DIR}/*.md"`
for file in ${ALL_DOC_CONTENT_FILES}
do
  relFile=${file##${OUTPUT_CONTENT_DIR}/}
  echo "$(cat <<EOT

---

Found a typo, inconsistency or missing information in our docs?
Help us to improve [Thanos](https://thanos.io) documentation by proposing a fix [on GitHub here](https://github.com/thanos-io/thanos/edit/master/docs/${relFile}) :heart:

EOT
)" >> ${file}

done

# Add headers to special CODE_OF_CONDUCT.md, CONTRIBUTING.md and CHANGELOG.md files.
echo "$(cat <<EOT
---
title: Code of Conduct
type: docs
menu: contributing
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md
cat CODE_OF_CONDUCT.md >> ${OUTPUT_CONTENT_DIR}/CODE_OF_CONDUCT.md

echo "$(cat <<EOT
---
title: Contributing
type: docs
menu: contributing
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md
cat CONTRIBUTING.md >> ${OUTPUT_CONTENT_DIR}/CONTRIBUTING.md

echo "$(cat <<EOT
---
title: Changelog
type: docs
menu: thanos
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/CHANGELOG.md
cat CHANGELOG.md >> ${OUTPUT_CONTENT_DIR}/CHANGELOG.md

echo "$(cat <<EOT
---
title: Maintainers
type: docs
menu: thanos
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/MAINTAINERS.md
cat MAINTAINERS.md >> ${OUTPUT_CONTENT_DIR}/MAINTAINERS.md

echo "$(cat <<EOT
---
title: Security
type: docs
menu: thanos
---
EOT
)" > ${OUTPUT_CONTENT_DIR}/SECURITY.md
cat SECURITY.md >> ${OUTPUT_CONTENT_DIR}/SECURITY.md

# Glob again to include new docs.
ALL_DOC_CONTENT_FILES=$(echo "${OUTPUT_CONTENT_DIR}/**/*.md ${OUTPUT_CONTENT_DIR}/*.md")

# All the absolute links are replaced with a direct link to the file on github, including the current commit SHA.
perl -pi -e 's/]\(\//]\(https:\/\/github.com\/thanos-io\/thanos\/tree\/'${COMMIT_SHA}'\//g' ${ALL_DOC_CONTENT_FILES}

# All the relative links needs to have ../  This is because Hugo is missing: https://github.com/gohugoio/hugo/pull/3934
perl -pi -e 's/]\((?!http)/]\(..\//g' ${ALL_DOC_CONTENT_FILES}
# All the relative links in src= needs to have ../ as well.
perl -pi -e 's/src=\"(?!http)/src=\"..\//g' ${ALL_DOC_CONTENT_FILES}


