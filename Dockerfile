FROM quay.io/prometheus/busybox:latest
LABEL maintainer="The Thanos Authors"

COPY thanos /bin/thanos

USER       nobody
ENTRYPOINT [ "/bin/thanos" ]
