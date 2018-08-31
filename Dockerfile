FROM quay.io/prometheus/busybox:latest
LABEL maintainer="The Thanos Authors"

COPY thanos /bin/thanos

ENTRYPOINT [ "/bin/thanos" ]
