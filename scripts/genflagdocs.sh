#!/usr/bin/env bash
#
# Generate --help output for all commands and embed them into the component docs.
set -e
set -u

EMBEDMD_BIN=${EMBEDMD_BIN:-embedmd}
SED_BIN=${SED_BIN:-sed}

function docs {
# if check arg was passed, instead of the docs generation verifies if docs coincide with the codebase
if [[ "${CHECK}" == "check" ]]; then
    set +e
    DIFF=$(${EMBEDMD_BIN} -d *.md)
    RESULT=$?
    if [[ "$RESULT" != "0" ]]; then
        cat << EOF
Docs have discrepancies, do 'make docs' and commit changes:

${DIFF}
EOF
        exit 2
    fi
else
    ${EMBEDMD_BIN} -w *.md
fi

}

if ! [[ "$0" =~ "scripts/genflagdocs.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

CHECK=${1:-}

commands=("compact" "query" "rule" "sidecar" "store" "bucket" "check")

for x in "${commands[@]}"; do
    ./thanos "${x}" --help &> "docs/components/flags/${x}.txt"
done

bucketCommands=("verify" "ls" "inspect" "web" "replicate" "downsample")
for x in "${bucketCommands[@]}"; do
    ./thanos bucket "${x}" --help &> "docs/components/flags/bucket_${x}.txt"
done

checkCommands=("rules")
for x in "${checkCommands[@]}"; do
    ./thanos check "${x}" --help &> "docs/components/flags/check_${x}.txt"
done

# remove white noise
${SED_BIN} -i -e 's/[ \t]*$//' docs/components/flags/*.txt

go run scripts/cfggen/main.go --output-dir=docs/flags

# Change dir so embedmd understand the local references made in our markdown doc.
pushd "docs/components" > /dev/null
  docs
popd > /dev/null

pushd "docs" > /dev/null
  docs
popd > /dev/null
