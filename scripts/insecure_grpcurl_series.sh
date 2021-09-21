#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

HELP='
insecure_grpcurl_series.sh allows you to use call StoreAPI.Series gRPC method and receive streamed series in JSON format.

Usage:
  # Start some example Thanos component that exposes gRPC or use existing one. To start example one run: `thanos query &`
  bash scripts/insecure_grpcurl_series.sh localhost:10901 '"'"'[{"type": 0, "name": "__name__", "value":"go_goroutines"}]'"'"' 0 10
'

STORE_API_HOSTPORT=$1
if [ -z "${STORE_API_HOSTPORT}" ]; then
  echo '$1 is missing (STORE_API_HOSTPORT). Expected host:port string for the target StoreAPI to grpcurl against, e.g. localhost:10901'
  echo "${HELP}"
  exit 1
fi

REQUESTED_MATCHERS=$2
if [ -z "${REQUESTED_MATCHERS}" ]; then
  echo '$2 is missing (REQUESTED_MATCHERS). Expected matchers in form of JSON matchers: e.g [{"type": 0, "name": "__name__", "value":"go_goroutines"}]'
  echo "${HELP}"
  exit 1
fi

REQUESTED_MIN_TIME=$3
if [ -z "${REQUESTED_MIN_TIME}" ]; then
  echo '$3 is missing (REQUESTED_MIN_TIME). Expected min time in unix_timestamp.'
  echo "${HELP}"
  exit 1
fi

REQUESTED_MAX_TIME=$4
if [ -z "${REQUESTED_MAX_TIME}" ]; then
  echo '$4 is missing (REQUESTED_MAX_TIME). Expected max time in unix_timestamp.'
  echo "${HELP}"
  exit 1
fi

go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.8.2

SERIES_REQUEST='{
  "min_time": '${REQUESTED_MIN_TIME}',
  "max_time": '${REQUESTED_MAX_TIME}',
  "matchers": '${REQUESTED_MATCHERS}',
  "max_resolution_window": 0,
  "aggregates": [],
  "partial_response_strategy": 0,
  "skip_chunks": false
}'

GOGOPROTO_ROOT="$(GO111MODULE=on go list -f '{{ .Dir }}' -m github.com/gogo/protobuf)"

cd $DIR/../pkg/ || exit
grpcurl \
  -import-path="${GOGOPROTO_ROOT}" \
  -import-path=. \
  -proto=store/storepb/rpc.proto \
  -plaintext \
  -d="${SERIES_REQUEST}" "${STORE_API_HOSTPORT}" thanos.Store/Series
