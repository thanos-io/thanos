#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -ex
set -u

PROTOC_VERSION=${PROTOC_VERSION:-3.20.0}
PROTOC_BIN=${PROTOC_BIN:-protoc}
GOIMPORTS_BIN=${GOIMPORTS_BIN:-goimports}
PROTOC_GO_INJECT_TAG_BIN=${PROTOC_GO_INJECT_TAG_BIN:-protoc-go-inject-tag}
PROTOC_GEN_GO_BIN=${PROTOC_GEN_GO_BIN:-protoc-gen-go}
PROTOC_GEN_GO_GRPC_BIN=${PROTOC_GEN_GO_GRPC_BIN:-protoc-gen-go-grpc}
PROTOC_GEN_GO_VTPROTO_BIN=${PROTOC_GEN_GO_VTPROTO_BIN:-protoc-gen-go-vtproto}
VTPROTOBUF_VERSION="$(go list -m all | grep 'github.com/planetscale/vtprotobuf' | awk '{ print $2 }')"
VTPROTOBUF_INCLUDE_PATH="$(go env GOMODCACHE)/github.com/planetscale/vtprotobuf@${VTPROTOBUF_VERSION}/include"

if ! [[ "scripts/genproto.sh" =~ $0 ]]; then
  echo "must be run from repository root"
  exit 255
fi

if ! [[ $(${PROTOC_BIN} --version) == *"${PROTOC_VERSION}"* ]]; then
  echo "could not find protoc ${PROTOC_VERSION}, is it installed + in PATH?"
  exit 255
fi

DIRS="store/storepb/ store/storepb/prompb/ store/labelpb rules/rulespb targets/targetspb store/hintspb queryfrontend metadata/metadatapb exemplars/exemplarspb info/infopb api/query/querypb"
INCLUDE_PATH=${INCLUDE_PATH:-/tmp/proto/include}
echo "generating code"
pushd "pkg"
for dir in ${DIRS}; do
  LIST=$(find "${dir}" -type f -name "*.proto" | awk '{printf "%s ", $0}')

  ${PROTOC_BIN} --plugin=protoc-gen-go=${PROTOC_GEN_GO_BIN} \
    --plugin=protoc-gen-go-grpc=${PROTOC_GEN_GO_GRPC_BIN} \
    --plugin=protoc-gen-go-vtproto=${PROTOC_GEN_GO_VTPROTO_BIN} \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative -I=. -I=${INCLUDE_PATH} -I=${VTPROTOBUF_INCLUDE_PATH} \
    --go_opt=Mstore/storepb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb \
    --go_opt=Mrules/rulespb/rpc.proto=github.com/thanos-io/thanos/pkg/rules/rulespb \
    --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+equal,paths=source_relative \
    --go_opt=Mstore/storepb/prompb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb/prompb \
    --go_opt=Mmetadata/metadatapb/rpc.proto=github.com/thanos-io/thanos/pkg/metadata/metadatapb \
    ${LIST}

  ${PROTOC_GO_INJECT_TAG_BIN} -input=${dir}/*pb.go
  ${GOIMPORTS_BIN} -w ${dir}/*pb.go
done
popd

# Generate vendored Cortex protobufs.
CORTEX_DIRS="cortex/querier/queryrange/ cortex/cortexpb/ cortex/querier/stats/"
pushd "internal"
for dir in ${CORTEX_DIRS}; do
  LIST=$(find "${dir}" -type f -name "*.proto" | awk '{printf "%s ", $0}')

  ${PROTOC_BIN} --plugin=protoc-gen-go=${PROTOC_GEN_GO_BIN} \
    --plugin=protoc-gen-go-grpc=${PROTOC_GEN_GO_GRPC_BIN} \
    --plugin=protoc-gen-go-vtproto=${PROTOC_GEN_GO_VTPROTO_BIN} \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative -I=. -I=${INCLUDE_PATH} -I=${VTPROTOBUF_INCLUDE_PATH} \
    --go_opt=Mstore/storepb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb \
    --go_opt=Mrules/rulespb/rpc.proto=github.com/thanos-io/thanos/pkg/rules/rulespb \
    --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+equal,paths=source_relative \
    --go_opt=Mstore/storepb/prompb/types.proto=github.com/thanos-io/thanos/pkg/store/storepb/prompb \
    ${LIST}

  ${PROTOC_GO_INJECT_TAG_BIN} -input=${dir}/*pb.go
  ${GOIMPORTS_BIN} -w ${dir}/*pb.go
done
popd
