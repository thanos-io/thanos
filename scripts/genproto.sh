#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -e
set -u

PROTOC_BIN=${PROTOC_BIN:-protoc}
GOIMPORTS_BIN=${GOIMPORTS_BIN:-goimports}
PROTOC_GEN_GOGOFAST_BIN=${PROTOC_GEN_GOGOFAST_BIN:-protoc-gen-gogofast}

if ! [[ "scripts/genproto.sh" =~ $0 ]]; then
  echo "must be run from repository root"
  exit 255
fi

if ! [[ $(${PROTOC_BIN} --version) == *"3.4.0"* ]]; then
  echo "could not find protoc 3.4.0, is it installed + in PATH?"
  exit 255
fi

mkdir -p /tmp/protobin/
cp ${PROTOC_GEN_GOGOFAST_BIN} /tmp/protobin/protoc-gen-gogofast
PATH=${PATH}:/tmp/protobin
GOGOPROTO_ROOT="$(GO111MODULE=on go list -modfile=.bingo/protoc-gen-gogofast.mod -f '{{ .Dir }}' -m github.com/gogo/protobuf)"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

DIRS="store/storepb/ store/storepb/prompb/ store/labelpb rules/rulespb store/hintspb queryfrontend"
echo "generating code"
pushd "pkg"
for dir in ${DIRS}; do
  ${PROTOC_BIN} --gogofast_out=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. \
    -I=. \
    -I="${GOGOPROTO_PATH}" \
    ${dir}/*.proto

  pushd ${dir}
  sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
  sed -i.bak -E 's/_ \"google\/protobuf\"//g' *.pb.go
  # We cannot do Mstore/storepb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb,\ due to protobuf v1 bug.
  # TODO(bwplotka): Consider removing in v2.
  sed -i.bak -E 's/\"store\/storepb\"/\"github.com\/thanos-io\/thanos\/pkg\/store\/storepb\"/g' *.pb.go
  sed -i.bak -E 's/\"store\/labelpb\"/\"github.com\/thanos-io\/thanos\/pkg\/store\/labelpb\"/g' *.pb.go
  sed -i.bak -E 's/\"store\/storepb\/prompb\"/\"github.com\/thanos-io\/thanos\/pkg\/store\/storepb\/prompb\"/g' *.pb.go
  rm -f *.bak
  ${GOIMPORTS_BIN} -w *.pb.go
  popd
done
popd
