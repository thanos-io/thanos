# Alternative to quay.io/prometheus/busybox
FROM prom/busybox:latest
LABEL maintainer="The Thanos Authors"

COPY /thanos_tmp_for_docker /bin/thanos

ENTRYPOINT [ "/bin/thanos" ]
