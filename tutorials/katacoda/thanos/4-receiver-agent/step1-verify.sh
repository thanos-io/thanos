#!/usr/bin/env bash

# receive
curl -s 127.0.0.1:10909/metrics >/dev/null || exit 1
# query
curl -s 127.0.0.1:39090/metrics >/dev/null || exit 1

echo '"done"'
