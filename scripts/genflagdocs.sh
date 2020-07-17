#!/usr/bin/env bash
#
# Generate --help output for all commands and embed them into the component docs.
set -e
set -u

EMBEDMD_BIN=${EMBEDMD_BIN:-embedmd}
SED_BIN=${SED_BIN:-sed}
THANOS_BIN=${THANOS_BIN:-${GOBIN}/thanos}

function docs() {
  # If check arg was passed, instead of the docs generation verifies if docs coincide with the codebase.
  if [[ ${CHECK} == "check" ]]; then
    set +e
    DIFF=$(${EMBEDMD_BIN} -d *.md)
    RESULT=$?
    if [[ $RESULT != "0" ]]; then
      cat <<EOF
Docs have discrepancies, do 'make docs' and commit changes:

${DIFF}
EOF
      exit 2
    fi
  else
    ${EMBEDMD_BIN} -w *.md
  fi

}

if ! [[ $0 == "scripts/genflagdocs.sh" ]]; then
  echo "must be run from repository root"
  exit 255
fi

CHECK=${1:-}

# Auto update flags.

commands=("compact" "query" "rule" "sidecar" "store" "receive" "tools" "query-frontend")
for x in "${commands[@]}"; do
  ${THANOS_BIN} "${x}" --help &>"docs/components/flags/${x}.txt"
done

toolsCommands=("bucket" "rules-check")
for x in "${toolsCommands[@]}"; do
  ${THANOS_BIN} tools "${x}" --help &>"docs/components/flags/tools_${x}.txt"
done

toolsBucketCommands=("verify" "ls" "inspect" "web" "replicate" "downsample")
for x in "${toolsBucketCommands[@]}"; do
  ${THANOS_BIN} tools bucket "${x}" --help &>"docs/components/flags/tools_bucket_${x}.txt"
done

# Remove white noise.
${SED_BIN} -i -e 's/[ \t]*$//' docs/components/flags/*.txt

# Auto update configuration.
go run scripts/cfggen/main.go --output-dir=docs/flags

# Change dir so embedmd understand the local references made in our markdown doc.
pushd "docs/components" >/dev/null
docs
popd >/dev/null

pushd "docs" >/dev/null
docs
popd >/dev/null
