#!/bin/sh
# NOTE(kiennt26): This is a test entrypoint script for the Thanos container.
# It is used to test the container's behavior when the THANOS_ULIMIT_NOFILES
# environment variable is set.
set -e

if [ -n "${THANOS_ULIMIT_NOFILES:-}" ]; then
  echo "Setting file description limit to $THANOS_ULIMIT_NOFILES"
  ulimit -n "$THANOS_ULIMIT_NOFILES"
  ulimit -Hn "$THANOS_ULIMIT_NOFILES"
fi

exec /bin/thanos "$@"
