FROM alpine:3.7
LABEL maintainer="The Thanos Authors"

# This allows CGO_ENABLED=1 if needed.
RUN apk add --no-cache \
        libc6-compat

COPY blockgen /bin/blockgen

ENTRYPOINT [ "/bin/blockgen" ]
