FROM golang:1.15-alpine3.12 as builder

WORKDIR $GOPATH/src/github.com/thanos-io/thanos
# Change in the docker context invalidates the cache so to leverage docker
# layer caching, moving update and installing apk packages above COPY cmd
# More info https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#leverage-build-cache
RUN apk update && apk upgrade && apk add --no-cache alpine-sdk
# Replaced ADD with COPY as add is generally to download content form link or tar files
# while COPY supports the basic copying of local files into the container.
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#add-or-copy
COPY . $GOPATH/src/github.com/thanos-io/thanos

RUN git update-index --refresh; make build

# -----------------------------------------------------------------------------

ARG SHA="0c38f63cbe19e40123668a48c36466ef72b195e723cbfcbe01e9657a5f14cec6"
FROM quay.io/prometheus/busybox@sha256:${SHA}
LABEL maintainer="The Thanos Authors"

COPY --from=builder /go/bin/thanos /bin/thanos

ENTRYPOINT [ "/bin/thanos" ]
