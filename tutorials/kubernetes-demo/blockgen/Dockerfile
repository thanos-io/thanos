FROM quay.io/prometheus/busybox:latest
LABEL maintainer="The Thanos Authors"

COPY blockgen /bin/blockgen

ENTRYPOINT [ "/bin/blockgen" ]
