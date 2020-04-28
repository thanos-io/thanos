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

echo "installing gogofast"
GO111MODULE=on go install "github.com/gogo/protobuf/protoc-gen-gogofast"

GOGOPROTO_ROOT="$(GO111MODULE=on go list -f '{{ .Dir }}' -m github.com/gogo/protobuf)"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

DIRS="pkg/store/storepb pkg/store/storepb/prompb pkg/store/hintspb"

echo "generating code"
for dir in ${DIRS}; do
	pushd ${dir}
		${PROTOC_BIN} --gogofast_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mprompb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb/prompb,\
plugins=grpc:. \
		  -I=. \
			-I="${GOGOPROTO_PATH}" \
			*.proto

		${GOIMPORTS_BIN} -w *.pb.go
	popd
done
