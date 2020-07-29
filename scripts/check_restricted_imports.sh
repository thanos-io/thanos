#!/usr/bin/env bash
#
# Checks that restricted packages are not being imported.
# Run from repository root.
#set -e
set -u

if ! [[ "$0" =~ "scripts/check_restricted_imports.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

exit_status=0

packages=(
  "sync/atomic"
)
for package in "${packages[@]}"
do
  # Grep exits with 0 if there is at least one match
  if go list -f '{{ join .Imports "\n" }}' ./... | grep -q "${package}"; then
    exit_status=1
    echo "Restricted package '${package}' is being used"
  fi
done
exit ${exit_status}