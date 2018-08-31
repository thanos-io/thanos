FROM golang:1.9.2-alpine3.6 AS build

RUN apk add --no-cache git
RUN apk add --no-cache make

COPY . /go/src/github.com/improbable-eng/thanos

WORKDIR /go/src/github.com/improbable-eng/thanos

RUN make test

RUN make

FROM quay.io/prometheus/busybox:latest
LABEL maintainer="The Thanos Authors"

COPY --from=build /go/src/github.com/improbable-eng/thanos/thanos /bin/thanos

ENTRYPOINT [ "/bin/thanos" ]
