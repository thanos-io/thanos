# By default we pin to amd64 sha. Use make docker to automatically adjust for arm64 versions.
ARG SHA="fca3819d670cdaee0d785499fda202ea01c0640ca0803d26ae6dbf2a1c8c041c"
FROM quay.io/prometheus/busybox@sha256:${SHA}
LABEL maintainer="The Thanos Authors"

COPY /thanos_tmp_for_docker /bin/thanos

ENTRYPOINT [ "/bin/thanos" ]
