# For details, see https://github.com/devcontainers/images/tree/main/src/go
FROM mcr.microsoft.com/devcontainers/go:1.22

RUN echo "Downloading prometheus..." \
    && curl -sSL -H "Accept: application/vnd.github.v3+json" "https://api.github.com/repos/prometheus/prometheus/releases" -o /tmp/releases.json \
    && VERSION_LIST="$(jq -r '.[] | select(.tag_name | contains("rc") | not) | .tag_name | split("v") | .[1]' /tmp/releases.json | tr -d '"' | sort -rV)" \
    && PROMETHEUS_LATEST_VERSION="$(echo "${VERSION_LIST}" | head -n 1)" \
    && PROMETHEUS_FILE_NAME="prometheus-${PROMETHEUS_LATEST_VERSION}.linux-amd64" \
    && curl -fsSLO "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_LATEST_VERSION}/${PROMETHEUS_FILE_NAME}.tar.gz" \
    && tar -xzf "${PROMETHEUS_FILE_NAME}.tar.gz" \
    && rm "${PROMETHEUS_FILE_NAME}.tar.gz" \
    && mv ${PROMETHEUS_FILE_NAME}/prometheus /go/bin/

ENV GOPROXY "https://proxy.golang.org"

COPY .devcontainer/welcome-message.txt /usr/local/etc/vscode-dev-containers/first-run-notice.txt
