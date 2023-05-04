#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -e
set -u

PROTOC_VERSION=${PROTOC_VERSION:-3.20.1}
PROTOC_BIN=${PROTOC_BIN:-protoc}
GOIMPORTS_BIN=${GOIMPORTS_BIN:-goimports}
PROTOC_GEN_GOGOFAST_BIN=${PROTOC_GEN_GOGOFAST_BIN:-protoc-gen-gogofast}

if ! [[ "scripts/genproto.sh" =~ $0 ]]; then
	echo "must be run from repository root"
	exit 255
fi

if ! [[ $(${PROTOC_BIN} --version) == *"${PROTOC_VERSION}"* ]]; then
	echo "could not find protoc ${PROTOC_VERSION}, is it installed + in PATH?"
	exit 255
fi

echo "generating code"
DIRS="store/storepb/ store/storepb/prompb/ store/labelpb rules/rulespb targets/targetspb store/hintspb queryfrontend metadata/metadatapb exemplars/exemplarspb info/infopb api/query/querypb"
pushd "pkg"
for dir in ${DIRS}; do
	${PROTOC_BIN} \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		-I=. \
		-I=/tmp/proto/include \
		${dir}/*.proto

	protoc-go-inject-tag -input=${dir}/*pb.go
	pushd ${dir}
	popd
done
popd
