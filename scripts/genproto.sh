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

DIRS="store/storepb/ store/storepb/prompb/ rules/rulespb store/hintspb"
echo "generating code"
pushd "pkg"
  for dir in ${DIRS}; do
    ${PROTOC_BIN} --gogofast_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
plugins=grpc:. \
		  -I=. \
			-I="${GOGOPROTO_PATH}" \
			${dir}/*.proto

    pushd ${dir}
      sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
      sed -i.bak -E 's/_ \"google\/protobuf\"//g' *.pb.go
      # We cannot do Mstore/storepb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb,\ due to protobuf v1 bug.
      # TODO(bwplotka): Consider removing in v2.
      sed -i.bak -E 's/\"store\/storepb\"/\"github.com\/thanos-io\/thanos\/pkg\/store\/storepb\"/g' *.pb.go
      sed -i.bak -E 's/\"store\/storepb\/prompb\"/\"github.com\/thanos-io\/thanos\/pkg\/store\/storepb\/prompb\"/g' *.pb.go
      rm -f *.bak
      ${GOIMPORTS_BIN} -w *.pb.go
    popd
  done
popd
