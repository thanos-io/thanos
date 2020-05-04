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
PROMETHEUS_ROOT="$(GO111MODULE=on go list -f '{{ .Dir }}' -m github.com/prometheus/prometheus)"


GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
PROMETHEUS_PATH="${PROMETHEUS_ROOT}:${PROMETHEUS_ROOT}/prompb"

DIRS="store/storepb/ rules/rulespb store/hintspb"
echo "generating code"
pushd "pkg"
  for dir in ${DIRS}; do
    ${PROTOC_BIN} --gogofast_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mprompb/types.proto=github.com/prometheus/prometheus/pkg/prompb,\
plugins=grpc:. \
		  -I=. \
			-I="${GOGOPROTO_PATH}" \
		  -I="${PROMETHEUS_ROOT}" \
			${dir}/*.proto

#    ${PROTOC_BIN} --gogofast_out=plugins=grpc:. \
#      -I=. \
#      -I="${GOGOPROTO_PATH}" \
#      -I="${PROMETHEUS_ROOT}" \
#      ${dir}/*.proto

    pushd ${dir}
#      sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
#      sed -i.bak -E 's/_ \"google\/protobuf\"//g' *.pb.go
#      sed -i.bak -E 's/\"prompb\"/\"github.com\/prometheus\/prometheus\/prompb\"/g' *.pb.go
#      sed -i.bak -E 's/\"store\/storepb\"/\"github.com\/thanos-io\/thanos\/pkg\/store\/storepb\"/g' *.pb.go
      rm -f *.bak
      ${GOIMPORTS_BIN} -w *.pb.go
    popd
  done
popd
