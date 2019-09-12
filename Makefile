PREFIX            ?= $(shell pwd)
FILES_TO_FMT      ?= $(shell find . -path ./vendor -prune -o -name '*.go' -print)

DOCKER_IMAGE_REPO ?= quay.io/thanos/thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)

TMP_GOPATH        ?= /tmp/thanos-go
GOBIN             ?= $(firstword $(subst :, ,${GOPATH}))/bin
GO111MODULE       ?= on
export GO111MODULE
GOPROXY           ?= https://proxy.golang.org
export GOPROXY

# Tools.
EMBEDMD           ?= $(GOBIN)/embedmd-$(EMBEDMD_VERSION)
# v2.0.0
EMBEDMD_VERSION   ?= 97c13d6e41602fc6e397eb51c45f38069371a969
LICHE             ?= $(GOBIN)/liche-$(LICHE_VERSION)
LICHE_VERSION     ?= 2a2e6e56f6c615c17b2e116669c4cdb31b5453f3
GOIMPORTS         ?= $(GOBIN)/goimports-$(GOIMPORTS_VERSION)
GOIMPORTS_VERSION ?= 9d4d845e86f14303813298ede731a971dd65b593
PROMU             ?= $(GOBIN)/promu-$(PROMU_VERSION)
PROMU_VERSION     ?= 9583e5a6448f97c6294dca72dd1d173e28f8d4a4
PROTOC            ?= $(GOBIN)/protoc-$(PROTOC_VERSION)
PROTOC_VERSION    ?= 3.4.0
# v0.55.3 This needs to match with version in netlify.toml
HUGO_VERSION      ?= 993b84333cd75faa224d02618f312a0e96b53372
HUGO              ?= $(GOBIN)/hugo-$(HUGO_VERSION)
# v3.1.1
GOBINDATA_VERSION ?= a9c83481b38ebb1c4eb8f0168fd4b10ca1d3c523
GOBINDATA         ?= $(GOBIN)/go-bindata-$(GOBINDATA_VERSION)
GIT               ?= $(shell which git)

GOLANGCILINT_VERSION ?= d2b1eea2c6171a1a1141a448a745335ce2e928a1
GOLANGCILINT         ?= $(GOBIN)/golangci-lint-$(GOLANGCILINT_VERSION)
MISSPELL_VERSION     ?= c0b55c8239520f6b5aa15a0207ca8b28027ba49e
MISSPELL             ?= $(GOBIN)/misspell-$(MISSPELL_VERSION)

WEB_DIR           ?= website
WEBSITE_BASE_URL  ?= https://thanos.io
PUBLIC_DIR        ?= $(WEB_DIR)/public
ME                ?= $(shell whoami)

# E2e test deps.
# Referenced by github.com/thanos-io/thanos/blob/master/docs/getting_started.md#prometheus

# Limited prom version, because testing was not possible. This should fix it: https://github.com/thanos-io/thanos/issues/758
PROM_VERSIONS           ?= v2.4.3 v2.5.0 v2.8.1 v2.9.2
PROMS ?= $(GOBIN)/prometheus-v2.4.3 $(GOBIN)/prometheus-v2.5.0 $(GOBIN)/prometheus-v2.8.1 $(GOBIN)/prometheus-v2.9.2

ALERTMANAGER_VERSION    ?= v0.15.2
ALERTMANAGER            ?= $(GOBIN)/alertmanager-$(ALERTMANAGER_VERSION)

MINIO_SERVER_VERSION    ?= RELEASE.2018-10-06T00-15-16Z
MINIO_SERVER            ?=$(GOBIN)/minio-$(MINIO_SERVER_VERSION)

# fetch_go_bin_version downloads (go gets) the binary from specific version and installs it in $(GOBIN)/<bin>-<version>
# arguments:
# $(1): Install path. (e.g github.com/campoy/embedmd)
# $(2): Tag or revision for checkout.
# TODO(bwplotka): Move to just using modules, however make sure to not use or edit Thanos go.mod file!
define fetch_go_bin_version
	@mkdir -p $(GOBIN)
	@mkdir -p $(TMP_GOPATH)

	@echo ">> fetching $(1)@$(2) revision/version"
	@if [ ! -d '$(TMP_GOPATH)/src/$(1)' ]; then \
    GOPATH='$(TMP_GOPATH)' GO111MODULE='off' go get -d -u '$(1)/...'; \
  else \
    CDPATH='' cd -- '$(TMP_GOPATH)/src/$(1)' && git fetch; \
  fi
	@CDPATH='' cd -- '$(TMP_GOPATH)/src/$(1)' && git checkout -f -q '$(2)'
	@echo ">> installing $(1)@$(2)"
	@GOBIN='$(TMP_GOPATH)/bin' GOPATH='$(TMP_GOPATH)' GO111MODULE='off' go install '$(1)'
	@mv -- '$(TMP_GOPATH)/bin/$(shell basename $(1))' '$(GOBIN)/$(shell basename $(1))-$(2)'
	@echo ">> produced $(GOBIN)/$(shell basename $(1))-$(2)"

endef

define require_clean_work_tree
	@git update-index -q --ignore-submodules --refresh

    @if ! git diff-files --quiet --ignore-submodules --; then \
        echo >&2 "cannot $1: you have unstaged changes."; \
        git diff-files --name-status -r --ignore-submodules -- >&2; \
        echo >&2 "Please commit or stash them."; \
        exit 1; \
    fi

    @if ! git diff-index --cached --quiet HEAD --ignore-submodules --; then \
        echo >&2 "cannot $1: your index contains uncommitted changes."; \
        git diff-index --cached --name-status -r --ignore-submodules HEAD -- >&2; \
        echo >&2 "Please commit or stash them."; \
        exit 1; \
    fi

endef

.PHONY: all
all: format build

# assets repacks all statis assets into go file for easier deploy.
.PHONY: assets
assets: $(GOBINDATA)
	@echo ">> deleting asset file"
	@rm pkg/ui/bindata.go || true
	@echo ">> writing assets"
	@$(GOBINDATA) $(bindata_flags) -pkg ui -o pkg/ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  pkg/ui/templates/... pkg/ui/static/...
	@go fmt ./pkg/ui


# build builds Thanos binary using `promu`.
.PHONY: build
build: check-git deps $(PROMU)
	@echo ">> building binaries $(GOBIN)"
	@$(PROMU) build --prefix $(PREFIX)

# crossbuild builds all binaries for all platforms.
.PHONY: crossbuild
crossbuild: $(PROMU)
	@echo ">> crossbuilding all binaries"
	$(PROMU) crossbuild -v

# deps ensures fresh go.mod and go.sum.
.PHONY: deps
deps:
	@go mod tidy
	@go mod verify

# docker builds docker with no tag.
.PHONY: docker
docker: build
	@echo ">> building docker image 'thanos'"
	@docker build -t "thanos" .

#docker-multi-stage builds docker image using multi-stage.
.PHONY: docker-multi-stage
docker-multi-stage:
	@echo ">> building docker image 'thanos' with Dockerfile.multi-stage"
	@docker build -f Dockerfile.multi-stage -t "thanos" .

# docker-push pushes docker image build under `thanos` to "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"
.PHONY: docker-push
docker-push:
	@echo ">> pushing image"
	@docker tag "thanos" "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"
	@docker push "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"

# docs regenerates flags in docs for all thanos commands.
.PHONY: docs
docs: $(EMBEDMD) build
	@EMBEDMD_BIN="$(EMBEDMD)" scripts/genflagdocs.sh

# check-docs checks if documentation have discrepancy with flags and if the links are valid.
.PHONY: check-docs
check-docs: $(EMBEDMD) $(LICHE) build
	@EMBEDMD_BIN="$(EMBEDMD)" scripts/genflagdocs.sh check
	@$(LICHE) --recursive docs --exclude "cloud.tencent.com" --document-root .
	@$(LICHE) --exclude "cloud.tencent.com|goreportcard.com" --document-root . *.md

# format formats the code (including imports format).
.PHONY: format
format: $(GOIMPORTS)
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(FILES_TO_FMT)

# proto generates golang files from Thanos proto files.
.PHONY: proto
proto: check-git  $(GOIMPORTS) $(PROTOC)
	@GOIMPORTS_BIN="$(GOIMPORTS)" PROTOC_BIN="$(PROTOC)" scripts/genproto.sh

.PHONY: promu
promu: $(PROMU)

.PHONY: tarballs-release
tarballs-release: $(PROMU)
	@echo ">> Publishing tarballs"
	$(PROMU) crossbuild -v tarballs
	$(PROMU) checksum -v .tarballs
	$(PROMU) release -v .tarballs

# test runs all Thanos golang tests against each supported version of Prometheus.
.PHONY: test
test: export GOCACHE= $(TMP_GOPATH)/gocache
test: export THANOS_TEST_MINIO_PATH= $(MINIO_SERVER)
test: export THANOS_TEST_PROMETHEUS_VERSIONS= $(PROM_VERSIONS)
test: export THANOS_TEST_ALERTMANAGER_PATH= $(ALERTMANAGER)
test: check-git install-deps
	@echo ">> install thanos GOOPTS=${GOOPTS}"
	# Thanos binary is required by e2e tests.
	@go install github.com/thanos-io/thanos/cmd/thanos
	# Be careful on GOCACHE. Those tests are sometimes using built Thanos/Prometheus binaries directly. Don't cache those.
	@rm -rf ${GOCACHE}
	@echo ">> running all tests. Do export THANOS_SKIP_GCS_TESTS='true' or/and THANOS_SKIP_S3_AWS_TESTS='true' or/and THANOS_SKIP_AZURE_TESTS='true' and/or THANOS_SKIP_SWIFT_TESTS='true' and/or THANOS_SKIP_TENCENT_COS_TESTS='true' if you want to skip e2e tests against real store buckets"
	@go test $(shell go list ./... | grep -v /vendor/);

.PHONY: test-only-gcs
test-only-gcs: export THANOS_SKIP_S3_AWS_TESTS = true
test-only-gcs: export THANOS_SKIP_AZURE_TESTS = true
test-only-gcs: export THANOS_SKIP_SWIFT_TESTS = true
test-only-gcs: export THANOS_SKIP_TENCENT_COS_TESTS = true
test-only-gcs:
	@echo ">> Skipping S3 tests"
	@echo ">> Skipping AZURE tests"
	@echo ">> Skipping SWIFT tests"
	@echo ">> Skipping TENCENT tests"
	$(MAKE) test

.PHONY: test-local
test-local: export THANOS_SKIP_GCS_TESTS = true
test-local:
	@echo ">> Skipping GCE tests"
	$(MAKE) test-only-gcs

# install-deps installs dependencies for e2e tetss.
# It installs supported versions of Prometheus and alertmanager to test against in e2e.
.PHONY: install-deps
install-deps: $(ALERTMANAGER) $(MINIO_SERVER) $(PROMS)
	@echo ">>GOBIN=$(GOBIN)"

.PHONY: docker-ci
# To be run by Thanos maintainer.
docker-ci: install-deps
	# Copy all to tmp local dir as this is required by docker.
	@rm -rf ./tmp/bin
	@mkdir -p ./tmp/bin
	@cp -r $(GOBIN)/* ./tmp/bin
	@docker build -t thanos-ci -f Dockerfile.thanos-ci .
	@echo ">> pushing thanos-ci image"
	@docker tag "thanos-ci" "quay.io/thanos/thanos-ci:v0.1.0"
	@docker push "quay.io/thanos/thanos-ci:v0.1.0"

# tooling deps. TODO(bwplotka): Pin them all to certain version!
.PHONY: check-git
check-git:
ifneq ($(GIT),)
	@test -x $(GIT) || (echo >&2 "No git executable binary found at $(GIT)."; exit 1)
else
	@echo >&2 "No git binary found."; exit 1
endif

.PHONY: web-pre-process
web-pre-process:
	@echo ">> running documentation website pre processing"
	@bash scripts/websitepreprocess.sh

.PHONY: web
web: web-pre-process $(HUGO)
	@echo ">> building documentation website"
	# TODO(bwplotka): Make it --gc
	@cd $(WEB_DIR) && HUGO_ENV=production $(HUGO) --config hugo.yaml --minify -v -b $(WEBSITE_BASE_URL)

.PHONY: lint
# PROTIP:
# Add
#      --cpu-profile-path string   Path to CPU profile output file
#      --mem-profile-path string   Path to memory profile output file
#
# to debug big allocations during linting.
lint: check-git $(GOLANGCILINT) $(MISSPELL)
	@echo ">> linting all of the Go files GOGC=${GOGC}"
	@$(GOLANGCILINT) run --enable goimports --enable goconst --skip-dirs vendor
	@echo ">> detecting misspells"
	@find . -type f | grep -v vendor/ | grep -vE '\./\..*' | xargs $(MISSPELL) -error

.PHONY: web-serve
web-serve: web-pre-process $(HUGO)
	@echo ">> serving documentation website"
	@cd $(WEB_DIR) && $(HUGO) --config hugo.yaml -v server

# non-phony targets
$(EMBEDMD):
	$(call fetch_go_bin_version,github.com/campoy/embedmd,$(EMBEDMD_VERSION))

$(GOIMPORTS):
	$(call fetch_go_bin_version,golang.org/x/tools/cmd/goimports,$(GOIMPORTS_VERSION))

$(LICHE):
	$(call fetch_go_bin_version,github.com/raviqqe/liche,$(LICHE_VERSION))

$(PROMU):
	$(call fetch_go_bin_version,github.com/prometheus/promu,$(PROMU_VERSION))

$(HUGO):
	@go get github.com/gohugoio/hugo@$(HUGO_VERSION)
	@mv $(GOBIN)/hugo $(HUGO)
	@go mod tidy

$(GOBINDATA):
	$(call fetch_go_bin_version,github.com/go-bindata/go-bindata/go-bindata,$(GOBINDATA_VERSION))

$(GOLANGCILINT):
	$(call fetch_go_bin_version,github.com/golangci/golangci-lint/cmd/golangci-lint,$(GOLANGCILINT_VERSION))

$(MISSPELL):
	$(call fetch_go_bin_version,github.com/client9/misspell/cmd/misspell,$(MISSPELL_VERSION))

$(ALERTMANAGER):
	$(call fetch_go_bin_version,github.com/prometheus/alertmanager/cmd/alertmanager,$(ALERTMANAGER_VERSION))

$(MINIO_SERVER):
	$(call fetch_go_bin_version,github.com/minio/minio,$(MINIO_SERVER_VERSION))

$(PROMS):
	$(foreach ver,$(PROM_VERSIONS),$(call fetch_go_bin_version,github.com/prometheus/prometheus/cmd/prometheus,$(ver)))

$(PROTOC):
	@mkdir -p $(TMP_GOPATH)
	@echo ">> fetching protoc@${PROTOC_VERSION}"
	@PROTOC_VERSION="$(PROTOC_VERSION)" TMP_GOPATH="$(TMP_GOPATH)" scripts/installprotoc.sh
	@echo ">> installing protoc@${PROTOC_VERSION}"
	@mv -- "$(TMP_GOPATH)/bin/protoc" "$(GOBIN)/protoc-$(PROTOC_VERSION)"
	@echo ">> produced $(GOBIN)/protoc-$(PROTOC_VERSION)"
