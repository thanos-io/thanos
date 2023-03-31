# For details, see https://github.com/devcontainers/images/tree/main/src/go
FROM mcr.microsoft.com/devcontainers/go:0-1.20

RUN echo "Downloading prometheus..." \
    && curl -sSL -H "Accept: application/vnd.github.v3+json" "https://api.github.com/repos/prometheus/prometheus/tags" -o /tmp/tags.json \
    && VERSION_LIST="$(jq -r '.[] | select(.name | contains("rc") | not) | .name | split("v") | .[1]' /tmp/tags.json | tr -d '"' | sort -rV)" \
    && PROMETHEUS_LATEST_VERSION="$(echo "${VERSION_LIST}" | head -n 1)" \
    && PROMETHEUS_FILE_NAME="prometheus-${PROMETHEUS_LATEST_VERSION}.linux-amd64" \
    && curl -fsSLO "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_LATEST_VERSION}/${PROMETHEUS_FILE_NAME}.tar.gz" \
    && tar -xzf "${PROMETHEUS_FILE_NAME}.tar.gz" \
    && rm "${PROMETHEUS_FILE_NAME}.tar.gz" \
    && mv ${PROMETHEUS_FILE_NAME}/prometheus /go/bin/

ENV GOPROXY "https://proxy.golang.org"

COPY .devcontainer/welcome-message.txt /usr/local/etc/vscode-dev-containers/first-run-notice.txt
