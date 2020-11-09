ARG SHA="0c38f63cbe19e40123668a48c36466ef72b195e723cbfcbe01e9657a5f14cec6"
FROM quay.io/prometheus/busybox@sha256:${SHA}
LABEL maintainer="The Thanos Authors"

COPY /thanos_tmp_for_docker /bin/thanos

ENTRYPOINT [ "/bin/thanos" ]
