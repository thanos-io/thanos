PREFIX            ?= $(shell pwd)
FILES_TO_FMT      ?= $(shell find . -path ./vendor -prune -o -name '*.go' -print)

DOCKER_IMAGE_REPO ?= quay.io/thanos/thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)
DOCKER_CI_TAG     ?= test

# Ensure everything works even if GOPATH is not set, which is often the case.
# The `go env GOPATH` will work for all cases for Go 1.8+.
GOPATH            ?= $(shell go env GOPATH)

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

GOJSONTOYAML_VERSION    ?= e8bd32d46b3d764bef60f12b3bada1c132c4be55
GOJSONTOYAML            ?= $(GOBIN)/gojsontoyaml-$(GOJSONTOYAML_VERSION)
# v0.14.0
JSONNET_VERSION         ?= 724650d358b67909a7bea00ea443e23afc3d2a17
JSONNET                 ?= $(GOBIN)/jsonnet-$(JSONNET_VERSION)
JSONNETFMT              ?= $(GOBIN)/jsonnetfmt-$(JSONNET_VERSION)
JSONNET_BUNDLER_VERSION ?= efe0c9e864431e93d5c3376bd5931d0fb9b2a296
JSONNET_BUNDLER         ?= $(GOBIN)/jb-$(JSONNET_BUNDLER_VERSION)
# Prometheus v2.14.0
PROMTOOL_VERSION        ?= edeb7a44cbf745f1d8be4ea6f215e79e651bfe19
PROMTOOL                ?= $(GOBIN)/promtool-$(PROMTOOL_VERSION)

# Support gsed on OSX (installed via brew), falling back to sed. On Linux
# systems gsed won't be installed, so will use sed as expected.
SED ?= $(shell which gsed 2>/dev/null || which sed)

MIXIN_ROOT              ?= mixin
THANOS_MIXIN            ?= mixin/thanos
JSONNET_VENDOR_DIR      ?= mixin/vendor

WEB_DIR           ?= website
WEBSITE_BASE_URL  ?= https://thanos.io
PUBLIC_DIR        ?= $(WEB_DIR)/public
ME                ?= $(shell whoami)

# E2e test deps.
# Referenced by github.com/thanos-io/thanos/blob/master/docs/getting_started.md#prometheus

# Limited prom version, because testing was not possible. This should fix it: https://github.com/thanos-io/thanos/issues/758
PROM_VERSIONS           ?= v2.4.3 v2.5.0 v2.8.1 v2.9.2 v2.13.0
PROMS ?= $(GOBIN)/prometheus-v2.4.3 $(GOBIN)/prometheus-v2.5.0 $(GOBIN)/prometheus-v2.8.1 $(GOBIN)/prometheus-v2.9.2 $(GOBIN)/prometheus-v2.13.0

ALERTMANAGER_VERSION    ?= v0.20.0
ALERTMANAGER            ?= $(GOBIN)/alertmanager-$(ALERTMANAGER_VERSION)

MINIO_SERVER_VERSION    ?= RELEASE.2018-10-06T00-15-16Z
MINIO_SERVER            ?=$(GOBIN)/minio-$(MINIO_SERVER_VERSION)

FAILLINT_VERSION        ?= v1.2.0
FAILLINT                ?=$(GOBIN)/faillint-$(FAILLINT_VERSION)

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

help: ## Displays help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: all
all: format build

.PHONY: assets
assets: # Repacks all static assets into go file for easier deploy.
assets: $(GOBINDATA)
	@echo ">> deleting asset file"
	@rm pkg/ui/bindata.go || true
	@echo ">> writing assets"
	@$(GOBINDATA) $(bindata_flags) -pkg ui -o pkg/ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  pkg/ui/templates/... pkg/ui/static/...
	@go fmt ./pkg/ui

.PHONY: build
build: ## Builds Thanos binary using `promu`.
build: check-git deps $(PROMU)
	@echo ">> building binaries $(GOBIN)"
	@$(PROMU) build --prefix $(PREFIX)

.PHONY: crossbuild
crossbuild: ## Builds all binaries for all platforms.
crossbuild: $(PROMU)
	@echo ">> crossbuilding all binaries"
	$(PROMU) crossbuild -v

.PHONY: deps
deps: ## Ensures fresh go.mod and go.sum.
	@go mod tidy
	@go mod verify

.PHONY: docker
docker: ## Builds 'thanos' docker with no tag.
docker: build
	@echo ">> building docker image 'thanos'"
	@docker build -t "thanos" .

.PHONY: docker-multi-stage
docker-multi-stage: ## Builds 'thanos' docker image using multi-stage.
docker-multi-stage:
	@echo ">> building docker image 'thanos' with Dockerfile.multi-stage"
	@docker build -f Dockerfile.multi-stage -t "thanos" .

.PHONY: docker-push
docker-push: ## Pushes 'thanos' docker image build to "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)".
docker-push:
	@echo ">> pushing image"
	@docker tag "thanos" "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"
	@docker push "$(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG)"

.PHONY: docs
docs: ## Regenerates flags in docs for all thanos commands.
docs: $(EMBEDMD) build
	@EMBEDMD_BIN="$(EMBEDMD)" SED_BIN="$(SED)" scripts/genflagdocs.sh
	@find . -type f -name "*.md" | SED_BIN="$(SED)" xargs scripts/cleanup-white-noise.sh

.PHONY: check-docs
check-docs: ## checks docs against discrepancy with flags, links, white noise.
check-docs: $(EMBEDMD) $(LICHE) build
	@EMBEDMD_BIN="$(EMBEDMD)" SED_BIN="$(SED)" scripts/genflagdocs.sh check
	@$(LICHE) --recursive docs --exclude "(couchdb.apache.org/bylaws.html|cloud.tencent.com|alibabacloud.com)" --document-root .
	@$(LICHE) --exclude "goreportcard.com" --document-root . *.md
	@find . -type f -name "*.md" | SED_BIN="$(SED)" xargs scripts/cleanup-white-noise.sh
	$(call require_clean_work_tree,"check documentation")

.PHONY: check-comments
check-comments: ## Checks Go code comments if they have trailing period (excludes protobuffers and vendor files). Comments with more than 3 spaces at beginning are omitted from the check, example: '//    - foo'.
	@printf ">> checking Go comments trailing periods\n\n\n"
	@./scripts/build-check-comments.sh

.PHONY: format
format: ## Formats Go code including imports and cleans up white noise.
format: $(GOIMPORTS) check-comments
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(FILES_TO_FMT)
	@SED_BIN="$(SED)" scripts/cleanup-white-noise.sh $(FILES_TO_FMT)

.PHONY: proto
proto: ## Generates Go files from Thanos proto files.
proto: check-git  $(GOIMPORTS) $(PROTOC)
	@GOIMPORTS_BIN="$(GOIMPORTS)" PROTOC_BIN="$(PROTOC)" scripts/genproto.sh

.PHONY: promu
promu: $(PROMU)

.PHONY: tarballs-release
tarballs-release: ## Build tarballs.
tarballs-release: $(PROMU)
	@echo ">> Publishing tarballs"
	$(PROMU) crossbuild -v tarballs
	$(PROMU) checksum -v .tarballs
	$(PROMU) release -v .tarballs

.PHONY: test
test: ## Runs all Thanos Go unit tests against each supported version of Prometheus. This excludes tests in ./test/e2e.
test: export GOCACHE= $(TMP_GOPATH)/gocache
test: export THANOS_TEST_MINIO_PATH= $(MINIO_SERVER)
test: export THANOS_TEST_PROMETHEUS_VERSIONS= $(PROM_VERSIONS)
test: export THANOS_TEST_ALERTMANAGER_PATH= $(ALERTMANAGER)
test: check-git install-deps
	@echo ">> install thanos GOOPTS=${GOOPTS}"
	@echo ">> running unit tests (without /test/e2e). Do export THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS if you want to skip e2e tests against all real store buckets. Current value: ${THANOS_TEST_OBJSTORE_SKIP}"
	@go test $(shell go list ./... | grep -v /vendor/ | grep -v /test/e2e);

.PHONY: test-ci
test-ci: ## Runs test for CI, so excluding object storage integrations that we don't have configured yet.
test-ci: export THANOS_TEST_OBJSTORE_SKIP=AZURE,SWIFT,COS,ALIYUNOSS
test-ci:
	@echo ">> Skipping ${THANOS_TEST_OBJSTORE_SKIP} tests"
	$(MAKE) test

.PHONY: test-local
test-local: ## Runs test excluding tests for ALL  object storage integrations.
test-local: export THANOS_TEST_OBJSTORE_SKIP=GCS,S3,AZURE,SWIFT,COS,ALIYUNOSS
test-local:
	$(MAKE) test

.PHONY: test-e2e
test-e2e: ## Runs all Thanos e2e docker-based e2e tests from test/e2e. Required access to docker daemon.
test-e2e: docker
	@echo ">> cleaning docker environment."
	@docker system prune -f --volumes
	@echo ">> cleaning e2e test garbage."
	@rm -rf ./test/e2e/e2e_integration_test*
	@echo ">> running /test/e2e tests."
	# NOTE(bwplotka):
	# * If you see errors on CI (timeouts), but not locally, try to add -parallel 1 to limit to single CPU to reproduce small 1CPU machine.
	@go test -failfast -timeout 10m -v ./test/e2e/...

.PHONY: install-deps
install-deps: ## Installs dependencies for integration tests. It installs supported versions of Prometheus and alertmanager to test against in integration tests.
install-deps: $(ALERTMANAGER) $(MINIO_SERVER) $(PROMS)
	@echo ">>GOBIN=$(GOBIN)"

.PHONY: docker-ci
docker-ci: ## Builds and pushes docker image used by our CI. This is done to cache our tools and dependencies. To be run by Thanos maintainer.
docker-ci: install-deps
	# Copy all to tmp local dir as this is required by docker.
	@rm -rf ./tmp/bin
	@mkdir -p ./tmp/bin
	@cp -r $(GOBIN)/* ./tmp/bin
	@docker build -t thanos-ci -f Dockerfile.thanos-ci .
	@echo ">> pushing thanos-ci image"
	@docker tag "thanos-ci" "quay.io/thanos/thanos-ci:$(DOCKER_CI_TAG)"
	@docker push "quay.io/thanos/thanos-ci:$(DOCKER_CI_TAG)"

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
web: ## Builds our website.
web: web-pre-process $(HUGO)
	@echo ">> building documentation website"
	# TODO(bwplotka): Make it --gc
	@cd $(WEB_DIR) && HUGO_ENV=production $(HUGO) --config hugo.yaml --minify -v -b $(WEBSITE_BASE_URL)

# PROTIP:
# Add
#      --cpu-profile-path string   Path to CPU profile output file
#      --mem-profile-path string   Path to memory profile output file
# to debug big allocations during linting.
lint: ## Runs various static analysis against our code.
lint: check-git deps $(GOLANGCILINT) $(MISSPELL) $(FAILLINT)
	$(call require_clean_work_tree,"detected not clean master before running lint")
	@echo ">> verifying modules being imported"
	@# TODO(bwplotka): Add, Printf, DefaultRegisterer, NewGaugeFunc and MustRegister once exception are accepted. Add fmt.{Errorf}=github.com/pkg/errors.{Errorf} once https://github.com/fatih/faillint/issues/10 is addressed.
	@$(FAILLINT) -paths "errors=github.com/pkg/errors,\
github.com/prometheus/tsdb=github.com/prometheus/prometheus/tsdb,\
github.com/prometheus/prometheus/pkg/testutils=github.com/thanos-io/thanos/pkg/testutil,\
github.com/prometheus/client_golang/prometheus.{DefaultGatherer,DefBuckets,NewUntypedFunc,UntypedFunc},\
github.com/prometheus/client_golang/prometheus.{NewCounter,NewCounterVec,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,\
NewHistorgram,NewHistogramVec,NewSummary,NewSummaryVec}=github.com/prometheus/client_golang/prometheus/promauto.{NewCounter,\
NewCounterVec,NewCounterVec,NewGauge,NewGaugeVec,NewGaugeFunc,NewHistorgram,NewHistogramVec,NewSummary,NewSummaryVec}" ./...
	@$(FAILLINT) -paths "fmt.{Print,Println,Sprint}" -ignore-tests ./...
	@echo ">> examining all of the Go files"
	@go vet -stdmethods=false ./pkg/... ./cmd/... && go vet doc.go
	@echo ">> linting all of the Go files GOGC=${GOGC}"
	@$(GOLANGCILINT) run
	@echo ">> detecting misspells"
	@find . -type f | grep -v vendor/ | grep -vE '\./\..*' | xargs $(MISSPELL) -error
	@echo ">> detecting white noise"
	@find . -type f \( -name "*.md" -o -name "*.go" \) | SED_BIN="$(SED)" xargs scripts/cleanup-white-noise.sh
	$(call require_clean_work_tree,"detected white noise")
	@echo ">> ensuring Copyright headers"
	@go run ./scripts/copyright
	$(call require_clean_work_tree,"detected files without copyright")

.PHONY: web-serve
web-serve: ## Builds and serves Thanos website on localhost.
web-serve: web-pre-process $(HUGO)
	@echo ">> serving documentation website"
	@cd $(WEB_DIR) && $(HUGO) --config hugo.yaml -v server

# Check https://github.com/coreos/prometheus-operator/blob/master/scripts/jsonnet/Dockerfile for the image.
JSONNET_CONTAINER_CMD:=docker run --rm \
		-u="$(shell id -u):$(shell id -g)" \
		-v "$(shell go env GOCACHE):/.cache/go-build" \
		-v "$(PWD):/go/src/github.com/thanos-io/thanos:Z" \
		-w "/go/src/github.com/thanos-io/thanos" \
		-e USER=deadbeef \
		-e GO111MODULE=on \
		quay.io/coreos/jsonnet-ci:release-0.36

.PHONY: examples-in-container
examples-in-container:
	@echo ">> Compiling and generating thanos-mixin"
	$(JSONNET_CONTAINER_CMD) make $(MFLAGS) JSONNET_BUNDLER='/go/bin/jb' jsonnet-vendor
	$(JSONNET_CONTAINER_CMD) make $(MFLAGS) \
		EMBEDMD='/go/bin/embedmd' \
		JSONNET='/go/bin/jsonnet' \
		JSONNET_BUNDLER='/go/bin/jb' \
		PROMTOOL='/go/bin/promtool' \
		GOJSONTOYAML='/go/bin/gojsontoyaml' \
		GOLANGCILINT='/go/bin/golangci-lint' \
		examples

.PHONY: examples
examples: jsonnet-vendor jsonnet-format $(EMBEDMD) ${THANOS_MIXIN}/README.md examples/alerts/alerts.md examples/alerts/alerts.yaml examples/alerts/rules.yaml examples/dashboards examples/tmp
	$(EMBEDMD) -w examples/alerts/alerts.md
	$(EMBEDMD) -w ${THANOS_MIXIN}/README.md

.PHONY: examples/tmp
examples/tmp:
	-rm -rf examples/tmp/
	-mkdir -p examples/tmp/
	$(JSONNET) -J ${JSONNET_VENDOR_DIR} -m examples/tmp/ ${THANOS_MIXIN}/separated_alerts.jsonnet | xargs -I{} sh -c 'cat {} | $(GOJSONTOYAML) > {}.yaml; rm -f {}' -- {}

.PHONY: examples/dashboards # to keep examples/dashboards/dashboards.md.
examples/dashboards: $(JSONNET) ${THANOS_MIXIN}/mixin.libsonnet ${THANOS_MIXIN}/defaults.libsonnet ${THANOS_MIXIN}/dashboards/*
	-rm -rf examples/dashboards/*.json
	$(JSONNET) -J ${JSONNET_VENDOR_DIR} -m examples/dashboards ${THANOS_MIXIN}/dashboards.jsonnet

examples/alerts/alerts.yaml: $(JSONNET) $(GOJSONTOYAML) ${THANOS_MIXIN}/mixin.libsonnet ${THANOS_MIXIN}/defaults.libsonnet ${THANOS_MIXIN}/alerts/*
	$(JSONNET) ${THANOS_MIXIN}/alerts.jsonnet | $(GOJSONTOYAML) > $@

examples/alerts/rules.yaml: $(JSONNET) $(GOJSONTOYAML) ${THANOS_MIXIN}/mixin.libsonnet ${THANOS_MIXIN}/defaults.libsonnet ${THANOS_MIXIN}/rules/*
	$(JSONNET) ${THANOS_MIXIN}/rules.jsonnet | $(GOJSONTOYAML) > $@

.PHONY: jsonnet-vendor
jsonnet-vendor: $(JSONNET_BUNDLER) $(MIXIN_ROOT)/jsonnetfile.json $(MIXIN_ROOT)/jsonnetfile.lock.json
	rm -rf ${JSONNET_VENDOR_DIR}
	cd ${MIXIN_ROOT} && $(JSONNET_BUNDLER) install

JSONNETFMT_CMD := $(JSONNETFMT) -n 2 --max-blank-lines 2 --string-style s --comment-style s

.PHONY: jsonnet-format
jsonnet-format: $(JSONNETFMT)
	find . -name 'vendor' -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNETFMT_CMD) -i

.PHONY: jsonnet-format-in-container
jsonnet-format-in-container:
	$(JSONNET_CONTAINER_CMD) find . -name 'vendor' -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- jsonnetfmt -n 2 --max-blank-lines 2 --string-style s --comment-style s -i

.PHONY: example-rules-lint
example-rules-lint: $(PROMTOOL) examples/alerts/alerts.yaml examples/alerts/rules.yaml
	$(PROMTOOL) check rules examples/alerts/alerts.yaml examples/alerts/rules.yaml
	$(PROMTOOL) test rules examples/alerts/tests.yaml

.PHONY: check-examples
check-examples: examples example-rules-lint
	$(call require_clean_work_tree,'all generated files should be committed,check examples')

.PHONY: examples-clean
examples-clean:
	rm -f examples/alerts/alerts.yaml
	rm -f examples/alerts/rules.yaml
	rm -f examples/dashboards/*.json
	rm -f examples/tmp/*.yaml

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

$(FAILLINT):
	$(call fetch_go_bin_version,github.com/fatih/faillint,$(FAILLINT_VERSION))

$(PROMS):
	$(foreach ver,$(PROM_VERSIONS),$(call fetch_go_bin_version,github.com/prometheus/prometheus/cmd/prometheus,$(ver)))

$(PROTOC):
	@mkdir -p $(TMP_GOPATH)
	@echo ">> fetching protoc@${PROTOC_VERSION}"
	@PROTOC_VERSION="$(PROTOC_VERSION)" TMP_GOPATH="$(TMP_GOPATH)" scripts/installprotoc.sh
	@echo ">> installing protoc@${PROTOC_VERSION}"
	@mv -- "$(TMP_GOPATH)/bin/protoc" "$(GOBIN)/protoc-$(PROTOC_VERSION)"
	@echo ">> produced $(GOBIN)/protoc-$(PROTOC_VERSION)"

$(JSONNET):
	$(call fetch_go_bin_version,github.com/google/go-jsonnet/cmd/jsonnet,$(JSONNET_VERSION))

$(JSONNETFMT):
	$(call fetch_go_bin_version,github.com/google/go-jsonnet/cmd/jsonnetfmt,$(JSONNET_VERSION))

$(GOJSONTOYAML):
	$(call fetch_go_bin_version,github.com/brancz/gojsontoyaml,$(GOJSONTOYAML_VERSION))

$(JSONNET_BUNDLER):
	$(call fetch_go_bin_version,github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb,$(JSONNET_BUNDLER_VERSION))

$(PROMTOOL):
	$(call fetch_go_bin_version,github.com/prometheus/prometheus/cmd/promtool,$(PROMTOOL_VERSION))
