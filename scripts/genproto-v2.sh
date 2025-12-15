#!/usr/bin/env bash
#
# Generate all protobuf bindings using protobuf v2 (google.golang.org/protobuf)
# with vtprotobuf optimizations for marshal/unmarshal/size/pool.
# Run from repository root.
set -e
set -u

PROTOC_BIN=${PROTOC_BIN:-protoc}
GOIMPORTS_BIN=${GOIMPORTS_BIN:-goimports}

if ! [[ "scripts/genproto-v2.sh" =~ $0 ]]; then
  echo "must be run from repository root"
  exit 255
fi

# Ensure protoc-gen-go and protoc-gen-go-grpc are installed
if ! command -v protoc-gen-go &> /dev/null; then
    echo "protoc-gen-go not found. Installing..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "protoc-gen-go-grpc not found. Installing..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

if ! command -v protoc-gen-go-vtproto &> /dev/null; then
    echo "protoc-gen-go-vtproto not found. Installing..."
    go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest
fi

# Add Go bin to PATH
export PATH="${PATH}:$(go env GOPATH)/bin"

# vtprotobuf features to enable
VTPROTO_FEATURES="marshal+unmarshal+size+pool+equal+clone"

echo "Generating protobuf code with protoc-gen-go v2 + vtprotobuf..."

# Generate pkg protos
pushd "pkg"

# First generate labelpb (no dependencies)
echo "Processing store/labelpb..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  store/labelpb/types.proto

# Generate storepb/prompb (depends on labelpb)
echo "Processing store/storepb/prompb..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  store/storepb/prompb/types.proto \
  store/storepb/prompb/remote.proto

# Generate storepb/types (depends on labelpb)
# Enable memory pooling for Series which is heavily used in streaming
echo "Processing store/storepb/types..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  --go-vtproto_opt=pool=github.com/thanos-io/thanos/pkg/store/storepb.Series \
  --go-vtproto_opt=pool=github.com/thanos-io/thanos/pkg/store/storepb.AggrChunk \
  -I=. \
  -I=/usr/include \
  store/storepb/types.proto

# Generate storepb/rpc (depends on storepb/types and prompb)
# Enable memory pooling for frequently used streaming messages
echo "Processing store/storepb/rpc..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  --go-vtproto_opt=pool=github.com/thanos-io/thanos/pkg/store/storepb.SeriesResponse \
  --go-vtproto_opt=pool=github.com/thanos-io/thanos/pkg/store/storepb.WriteRequest \
  -I=. \
  -I=/usr/include \
  store/storepb/rpc.proto

# Generate hintspb (depends on storepb/types)
echo "Processing store/hintspb..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  store/hintspb/hints.proto

# Generate remaining packages
GRPC_DIRS="rules/rulespb targets/targetspb metadata/metadatapb exemplars/exemplarspb info/infopb api/query/querypb status/statuspb"
for dir in ${GRPC_DIRS}; do
  echo "Processing ${dir}..."
  
  ${PROTOC_BIN} \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
    --go-vtproto_opt=features=${VTPROTO_FEATURES} \
    -I=. \
    -I=/usr/include \
    ${dir}/*.proto
done

# Generate queryfrontend (no gRPC service)
echo "Processing queryfrontend..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  queryfrontend/response.proto

# Run goimports on all generated files
for dir in store/labelpb store/storepb store/storepb/prompb store/hintspb rules/rulespb targets/targetspb metadata/metadatapb exemplars/exemplarspb info/infopb api/query/querypb status/statuspb queryfrontend; do
  pushd ${dir}
  ${GOIMPORTS_BIN} -w *.pb.go 2>/dev/null || true
  ${GOIMPORTS_BIN} -w *_vtproto.pb.go 2>/dev/null || true
  popd
done

popd

# Generate vendored Cortex protobufs
echo "Generating Cortex protobufs..."
pushd "internal"

# Generate cortexpb first (no dependencies on pkg)
echo "Processing cortex/cortexpb..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  cortex/cortexpb/cortex.proto

# Generate queryrange (depends on cortexpb)
echo "Processing cortex/querier/queryrange..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  cortex/querier/queryrange/queryrange.proto

# Generate stats
echo "Processing cortex/querier/stats..."
${PROTOC_BIN} \
  --go_out=. --go_opt=paths=source_relative \
  --go-vtproto_out=. --go-vtproto_opt=paths=source_relative \
  --go-vtproto_opt=features=${VTPROTO_FEATURES} \
  -I=. \
  -I=/usr/include \
  cortex/querier/stats/stats.proto

# Run goimports on generated files
for dir in cortex/cortexpb cortex/querier/queryrange cortex/querier/stats; do
  pushd ${dir}
  ${GOIMPORTS_BIN} -w *.pb.go 2>/dev/null || true
  ${GOIMPORTS_BIN} -w *_vtproto.pb.go 2>/dev/null || true
  popd
done

popd

echo "Done generating protobuf code."
