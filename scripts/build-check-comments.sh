#!/usr/bin/env bash

set -eu

# Reliable cross-platform comment checker for go files.
# Uses docker and stable linux distro for reliable grep regex and
# to make sure we work on Linux and OS X and oh even Windows! :)
# Main issue is minor incompatibies between grep on various platforms.
#
# Linux: set USE_DOCKER=yes or USE_DOCKER=false to force run in docker
# All other platforms: uses docker by default. Set USE_DOCKER=no or USE_DOCKER=false to override.
#
# Checks Go code comments if they have trailing period (excludes protobuffers and vendor files).
# Comments with more than 3 spaces at beginning are omitted from the check, example: '//    - foo'.
# This does not include top-level commments for funcs and types.
#
# Example:
#   func main() {
#	    // comment without period, will trigger check
#	    //comment without leading space, will trigger check.
#	    // comment without trailing space, will trigger check.
#	    // good comment, leading space, ends with period, no trailing space.
#     //     - more than 3 leading spaces, will pass
#	    app := kingpin.New(filepath.Base(os.Args[0]), "A block storage based long-term storage for Prometheus")
#   }


# Abs path to project dir and this script, should work on all OS's
declare ROOT_DIR="$(cd $(dirname "${BASH_SOURCE}")/.. && pwd)"
declare THIS_SCRIPT="$(cd $(dirname "${BASH_SOURCE}") && pwd)/$(basename "${BASH_SOURCE}")"

# Image to use if we do docker-based commands. NB: busybox is no good for this.
declare IMAGE="debian:9-slim"

# User can explicitly ask to run in docker
declare USE_DOCKER=${USE_DOCKER:=""}

# For OS X, always use Docker as we have nasty
# compat GNU/BSG issues with grep.
if test "Linux" != "$(uname || true)"
then
  # Allow overriding for non-linux platforms
  if test "no" != "${USE_DOCKER}" && test "false" != "${USE_DOCKER}"
  then
    USE_DOCKER="yes"
  fi
fi

if test "yes" == "${USE_DOCKER}" || test "true" == "${USE_DOCKER}"
then
    # Make sure we only attach TTY if we have it, CI builds won't have it.
    declare TTY_FLAG=""
    if [ -t 1 ]
    then
        TTY_FLAG="-t"
    fi

    # Annoying issue with ownership of files in mapped volumes.
    # Need to run with same UID and GID in container as we do
    # on the machine, otherwise all output will be owned by root.
    # Doesn't happen on OS X but does on Linux. So we will do
    # UID and GID for Linux only (this won't work on OS X anyway).
    declare USER_FLAG=""
    if test "Linux" == "$(uname || true)"
    then
        USER_FLAG="-u $(id -u):$(id -g)"
    fi

    printf "\n\n\n This will run in Docker. \n If you get an error, ensure Docker is installed. \n\n\n"
    (
      set -x
      docker run \
              -i \
              ${TTY_FLAG} \
              ${USER_FLAG} \
              --rm \
              -v "${ROOT_DIR}":"${ROOT_DIR}":cached \
              -w "${ROOT_DIR}" \
              "${IMAGE}" \
              "${THIS_SCRIPT}"
    )
    exit 0
fi

function check_comments {
    # no bombing out on errors with grep
    set +e

    # This is quite mad but don't fear the https://regex101.com/ helps a lot.
    grep -Przo --color --include \*.go --exclude \*.pb.go --exclude bindata.go --exclude-dir vendor \
     '\n.*\s+//(\s{0,3}[^\s^+][^\n]+[^.?!:]{2}|[^\s].*)\n[ \t]*[^/\s].*\n' ./
    res=$?
    set -e

    # man grep: Normally, the exit status is 0 if selected lines are found and 1 otherwise.
    # But the exit status is 2 if an error occurred, unless the -q or --quiet or --silent
    # option is used and a selected line is found.
    if test "0" == "${res}"  # found something
    then
      printf "\n\n\n Error: Found comments without trailing period. Comments has to be full sentences.\n\n\n"
      exit 1
    elif test "1" == "${res}" # nothing found, all clear
    then
      printf "\n\n\n All comment formatting is good, Spartan.\n\n\n"
      exit 0
    else  # grep error
      printf "\n\n\n Hmmm something didn't work, issues with grep?.\n\n\n"
      exit 2
    fi
}

check_comments
