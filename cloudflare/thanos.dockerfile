ARG CLOUDFLARE_DOCKER_REGISTRY_HOST=registry.cfdata.org
FROM ${CLOUDFLARE_DOCKER_REGISTRY_HOST}/stash/plat/dockerfiles/debian-trixie-golang/master:1.27.3-1@sha256:521137a205e8e2efcbbc933d2f28d9ef05e14be4a75d16e34dbb73939f0f653f AS builder

# VULN-181166: require the CF trust-store update, not just a newer Go compiler.
RUN dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge 20260816+cf1

COPY . /workdir
RUN cd /workdir && PREFIX=/workdir make build

FROM gcr.io/distroless/base-nossl-debian13:nonroot@sha256:83df5862465415c1d5f3ade85adc4d58a922db94e0b4d26c83a1bf96d5adf72d
# Replace the public image's CA bundle with the patched CF bundle. Copy the
# bundle rather than symlinks whose targets live outside /etc/ssl/certs.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /workdir/thanos /usr/bin/thanos
ENTRYPOINT [ "/usr/bin/thanos" ]
