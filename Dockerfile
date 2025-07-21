# By default we pin to amd64 sha. Use make docker to automatically adjust for arm64 versions.
ARG BASE_DOCKER_SHA="14d68ca3d69fceaa6224250c83d81d935c053fb13594c811038c461194599973"
FROM quay.io/prometheus/busybox@sha256:${BASE_DOCKER_SHA}
LABEL maintainer="The Thanos Authors"

COPY /thanos_tmp_for_docker /bin/thanos
COPY docker-entrypoint.sh /docker-entrypoint.sh

RUN adduser \
    -D `#Dont assign a password` \
    -H `#Dont create home directory` \
    -u 1001 `#User id`\
    thanos && \
    chown thanos /bin/thanos && \
    chown thanos /docker-entrypoint.sh && \
    chmod +x /docker-entrypoint.sh
USER 1001
ENTRYPOINT [ "/docker-entrypoint.sh" ]
