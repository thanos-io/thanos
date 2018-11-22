#!/usr/bin/env sh

set -e

[ $# -eq 2 ] || {
  echo expected exactly two arguments, got "$#" 1>&2
  exit 1
}

case "$(go version)" in
*" go1.11 "*) ;;
*" go1.11."*) ;;
*)
  echo unsupported go version, needs 1.11 1>&2
  exit 1
  ;;
esac

FETCH_DIR="$(mktemp -d)" || {
  echo failed to create temporary directory 1>&2
  exit 1
}

# shellcheck disable=SC2064
trap "rm -rf -- \"$FETCH_DIR\"" INT EXIT

curl -sSL "https://codeload.github.com/colinmarc/hdfs/tar.gz/$1" |
  tar xz --strip-components=1 -C"$FETCH_DIR"

(
  CDPATH='' cd -- "$FETCH_DIR"
  go mod vendor
)

tar cz -C"$FETCH_DIR" -f"$2" .
