#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -e
set -u

PROTOC_BIN=${PROTOC_BIN:-protoc}
GOIMPORTS_BIN=${GOIMPORTS_BIN:-goimports}

if ! [[ "$0" =~ "scripts/genproto.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

if ! [[ $(${PROTOC_BIN} --version) =~ "3.4.0" ]]; then
	echo "could not find protoc 3.4.0, is it installed + in PATH?"
	exit 255
fi

THANOS_ROOT="${GOPATH}/src/github.com/improbable-eng/thanos"
PROM_PATH="${THANOS_ROOT}/pkg/store/storepb"
GOGOPROTO_ROOT="${THANOS_ROOT}/vendor/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

DIRS="pkg/store/storepb pkg/store/prompb"

for dir in ${DIRS}; do
	pushd ${dir}
		${PROTOC_BIN} --gogofast_out=plugins=grpc:. -I=. \
            -I="${GOGOPROTO_PATH}" \
            -I="${PROM_PATH}" \
            *.proto

		sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
		sed -i.bak -E 's/import _ \"google\/protobuf\"//g' *.pb.go
		rm -f *.bak
		${GOIMPORTS_BIN} -w *.pb.go
	popd
done
