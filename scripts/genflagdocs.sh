#!/usr/bin/env bash
#
# Generate --help output for all commands and embed them into the component docs.
set -e
set -u

function docs {
# if check arg was passed, instead of the docs generation verifies if docs coincide with the codebase
if [[ "${CHECK}" == "check" ]]; then
    set +e
    DIFF=$(embedmd -d *.md)
    RESULT=$?
    if [[ "$RESULT" != "0" ]]; then
        cat << EOF
Docs have discrepancies, do 'make docs' and commit changes:

${DIFF}
EOF
        exit 2
    fi
else
    embedmd -w *.md
fi

}

if ! [[ "$0" =~ "scripts/genflagdocs.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

CHECK=${1:-}

commands=("compact" "query" "rule" "sidecar" "store" "bucket")

for x in "${commands[@]}"; do
    ./thanos "${x}" --help &> "docs/components/flags/${x}.txt"
done

bucketCommands=("verify" "ls")
for x in "${bucketCommands[@]}"; do
    ./thanos bucket "${x}" --help &> "docs/components/flags/bucket_${x}.txt"
done

go run scripts/bucketcfggen/main.go --output-dir=docs/flags

# Change dir so embedmd understand the local references made in our markdown doc.
pushd "docs/components" > /dev/null
  docs
popd > /dev/null

pushd "docs" > /dev/null
  docs
popd > /dev/null
