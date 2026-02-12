ARG CLOUDFLARE_DOCKER_REGISTRY_HOST=registry.cfdata.org
FROM ${CLOUDFLARE_DOCKER_REGISTRY_HOST}/stash/plat/dockerfiles/debian-trixie-golang/master:1.25.5-1@sha256:0b03e1501a3cc43bdf32a3b52506f48a470110ee4b16408c69a71dd931db98f0 AS builder

COPY . /workdir
RUN cd /workdir && PREFIX=/workdir make build

FROM gcr.io/distroless/base-nossl-debian12:nonroot
COPY --from=builder /workdir/thanos /usr/bin/thanos
ENTRYPOINT [ "/usr/bin/thanos" ]
