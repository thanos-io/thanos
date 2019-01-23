#!/usr/bin/env bash
#
# Install the standard protocol buffer implementation - protoc.
set -e
set -u

PROTOC_VERSION=${PROTOC_VERSION:-3.4.0}
TMP_GOPATH=${TMP_GOPATH:-/tmp/thanos-go}
PROTOC_DOWNLOAD_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}"

OS=$(go env GOOS)
ARCH=$(go env GOARCH)
PLATFORM="${OS}/${ARCH}"

is_supported_platform() {
    platform=$1
    found=1
    case "$platform" in
        darwin/amd64) found=0 ;;
        darwin/i386) found=0 ;;
        linux/amd64) found=0 ;;
        linux/i386) found=0 ;;
        linux/arm64) found=0 ;;
    esac
    return $found
}

adjust_os() {
    case ${OS} in
        darwin) OS=osx ;;
    esac
    true
}

adjust_arch() {
    case ${ARCH} in
        amd64) ARCH=x86_64 ;;
        i386) ARCH=x86_32 ;;
        arm64) ARCH=aarch_64 ;;
    esac
    true
}

mkdir -p ${TMP_GOPATH}

is_supported_platform "$PLATFORM"
if [[ $? -eq 1 ]]; then
    echo "platform $PLATFORM is not supported. See https://github.com/protocolbuffers/protobuf/releases for details"
    exit 1
fi

adjust_os

adjust_arch

PACKAGE="protoc-${PROTOC_VERSION}-${OS}-${ARCH}.zip"
PACKAGE_DOWNLOAD_URL="${PROTOC_DOWNLOAD_URL}/${PACKAGE}"
curl -LSs ${PACKAGE_DOWNLOAD_URL} -o ${TMP_GOPATH}/${PACKAGE}
unzip -qqj ${TMP_GOPATH}/${PACKAGE} "bin/protoc" -d "${TMP_GOPATH}/bin/"
