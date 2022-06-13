#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -e
set -u

PROTOC_BIN=${PROTOC_BIN:-protoc}
GOIMPORTS_BIN=${GOIMPORTS_BIN:-goimports}
PROTOC_GEN_GOGOFAST_BIN=${PROTOC_GEN_GOGOFAST_BIN:-protoc-gen-gogofast}
INCLUDE_PATH=${INCLUDE_PATH:-/tmp/proto/include}
if ! [[ "scripts/genproto.sh" =~ $0 ]]; then
  echo "must be run from repository root"
  exit 255
fi

if ! [[ $(${PROTOC_BIN} --version) == *"3.20.0"* ]]; then
  echo "could not find protoc 3.20.0, is it installed + in PATH?"
  exit 255
fi

mkdir -p /tmp/protobin/
cp ${PROTOC_GEN_GOGOFAST_BIN} /tmp/protobin/protoc-gen-gogofast
cp ${PROTOC_GEN_GO_BIN} /tmp/protobin/protoc-gen-go
PATH=${PATH}:/tmp/protobin

DIRS="store/storepb store/storepb/prompb/ store/labelpb rules/rulespb targets/targetspb store/hintspb queryfrontend metadata/metadatapb exemplars/exemplarspb info/infopb"
echo "generating code"
pushd "pkg"
for dir in ${DIRS}; do

  LIST=$(find "${dir}" -type f -name "*.proto" | awk '{printf "%s ", $0}')


  ${PROTOC_BIN} --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative -I=. -I=${INCLUDE_PATH} \
  --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size,paths=source_relative \
  --go_opt=Mstore/storepb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb \
  --go_opt=Mrules/rulespb/rpc.proto=github.com/thanos-io/thanos/pkg/rules/rulespb \
  --go_opt=Mstore/storepb/prompb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb/prompb \
  ${LIST}

  # query-frontend uses these methods with different types
  # so remove them.
  sed -i -e '/.*ThanosLabelsResponse.*GetHeaders.*/,+6d' ${dir}/*pb.go
  sed -i -e '/.*ThanosSeriesResponse.*GetHeaders.*/,+6d' ${dir}/*pb.go




  protoc-go-inject-tag -input=${dir}/*pb.go


  pushd ${dir}

  ${GOIMPORTS_BIN} -w *.pb.go
  popd
done
popd
