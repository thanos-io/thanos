#!/usr/bin/env bash

curl -s 127.0.0.1:9000/metrics >/dev/null || exit 1

echo '"done"'
