#!/bin/sh

# Resources limits: maximum number of open file descriptors
if [ -n "${THANOS_ULIMIT_NOFILES:-}" ]; then
    current_limit=$(ulimit -n)
    if [ "$current_limit" != "unlimited" ]; then
        # shellcheck disable=SC2086
        if [ $THANOS_ULIMIT_NOFILES -gt $current_limit ]; then
            echo "Setting file description limit to $THANOS_ULIMIT_NOFILES"
            ulimit -n $THANOS_ULIMIT_NOFILES
        fi
    fi
fi

exec /bin/thanos "$@"
