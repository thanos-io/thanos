PREFIX            ?= $(shell pwd)
FILES_TO_FMT      ?= $(shell find . -path ./vendor -prune -o -name '*.go' -print)

DOCKER_IMAGE_NAME ?= thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)

TMP_GOPATH        ?= /tmp/thanos-go
GOBIN             ?= ${GOPATH}/bin
GO111MODULE       ?= on
export GO111MODULE

# Tools.
EMBEDMD           ?= $(GOBIN)/embedmd-$(EMBEDMD_VERSION)
# v2.0.0
EMBEDMD_VERSION   ?= 97c13d6e41602fc6e397eb51c45f38069371a969
ERRCHECK          ?= $(GOBIN)/errcheck-$(ERRCHECK_VERSION)
# v1.2.0
ERRCHECK_VERSION  ?= e14f8d59a22d460d56c5ee92507cd94c78fbf274
LICHE             ?= $(GOBIN)/liche-$(LICHE_VERSION)
LICHE_VERSION     ?= 2a2e6e56f6c615c17b2e116669c4cdb31b5453f3
GOIMPORTS         ?= $(GOBIN)/goimports-$(GOIMPORTS_VERSION)
GOIMPORTS_VERSION ?= 9d4d845e86f14303813298ede731a971dd65b593
PROMU             ?= $(GOBIN)/promu-$(PROMU_VERSION)
# v0.2.0
PROMU_VERSION     ?= 264dc36af9ea3103255063497636bd5713e3e9c1
PROTOC            ?= $(GOBIN)/protoc-$(PROTOC_VERSION)
PROTOC_VERSION    ?= 3.4.0
# v0.55.3 This needs to match with version in netlify.toml
HUGO_VERSION      ?= 993b84333cd75faa224d02618f312a0e96b53372
HUGO              ?= $(GOBIN)/hugo-$(HUGO_VERSION)
# v3.1.1
GOBINDATA_VERSION ?= a9c83481b38ebb1c4eb8f0168fd4b10ca1d3c523
GOBINDATA         ?= $(GOBIN)/go-bindata-$(GOBINDATA_VERSION)
GIT               ?= $(shell which git)

WEB_DIR           ?= website
WEBSITE_BASE_URL  ?= https://thanos.io
PUBLIC_DIR        ?= $(WEB_DIR)/public
ME                ?= $(shell whoami)

# E2e test deps.
# Referenced by github.com/improbable-eng/thanos/blob/master/docs/getting_started.md#prometheus

# Limited prom version, because testing was not possible. This should fix it: https://github.com/improbable-eng/thanos/issues/758
PROM_VERSIONS           ?=v2.4.3 v2.5.0 v2.8.1

ALERTMANAGER_VERSION    ?=v0.15.2
MINIO_SERVER_VERSION    ?=RELEASE.2018-10-06T00-15-16Z

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
build: check-git  go-mod-tidy $(PROMU)
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

# docker builds docker with no tag.
.PHONY: docker
docker: build
	@echo ">> building docker image '${DOCKER_IMAGE_NAME}'"
	@docker build -t "${DOCKER_IMAGE_NAME}" .

#docker-multi-stage builds docker image using multi-stage.
.PHONY: docker-multi-stage
docker-multi-stage:
	@echo ">> building docker image '${DOCKER_IMAGE_NAME}' with Dockerfile.multi-stage"
	@docker build -f Dockerfile.multi-stage -t "${DOCKER_IMAGE_NAME}" .

# docker-push pushes docker image build under `${DOCKER_IMAGE_NAME}` to improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"
.PHONY: docker-push
docker-push:
	@echo ">> pushing image"
	@docker tag "${DOCKER_IMAGE_NAME}" improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"
	@docker push improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"

# docs regenerates flags in docs for all thanos commands.
.PHONY: docs
docs: $(EMBEDMD) build
	@EMBEDMD_BIN="$(EMBEDMD)" scripts/genflagdocs.sh

# check-docs checks if documentation have discrepancy with flags and if the links are valid.
.PHONY: check-docs
check-docs: $(EMBEDMD) $(LICHE) build
	@EMBEDMD_BIN="$(EMBEDMD)" scripts/genflagdocs.sh check
	@$(LICHE) --recursive docs --exclude "cloud.tencent.com" --document-root .
	@$(LICHE) --exclude "cloud.tencent.com" --document-root . *.md

# errcheck performs static analysis and returns error if any of the errors is not checked.
.PHONY: errcheck
errcheck: $(ERRCHECK)
	@echo ">> errchecking the code"
	$(ERRCHECK) -verbose -exclude .errcheck_excludes.txt ./cmd/... ./pkg/... ./test/...

# format formats the code (including imports format).
# NOTE: format requires deps to not remove imports that are used, just not resolved.
# This is not encoded, because it is often used in IDE onSave logic.
.PHONY: format
format: $(GOIMPORTS)
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(FILES_TO_FMT)

# proto generates golang files from Thanos proto files.
.PHONY: proto
proto: check-git  $(GOIMPORTS) $(PROTOC)
	@go install ./vendor/github.com/gogo/protobuf/protoc-gen-gogofast
	@GOIMPORTS_BIN="$(GOIMPORTS)" PROTOC_BIN="$(PROTOC)" scripts/genproto.sh

.PHONY: promu
promu: $(PROMU)

# tarball builds release tarball.
.PHONY: tarball
tarball: $(PROMU)
	@echo ">> building release tarball"
	$(PROMU) tarball --prefix $(PREFIX) $(GOBIN)

.PHONY: tarballs-release
tarballs-release: $(PROMU)
	@echo ">> Publishing tarballs"
	$(PROMU) crossbuild tarballs
	$(PROMU) checksum .tarballs
	$(PROMU) release .tarballs

# test runs all Thanos golang tests against each supported version of Prometheus.
.PHONY: test
test: check-git test-deps
	@echo ">> running all tests. Do export THANOS_SKIP_GCS_TESTS='true' or/and THANOS_SKIP_S3_AWS_TESTS='true' or/and THANOS_SKIP_AZURE_TESTS='true' and/or THANOS_SKIP_SWIFT_TESTS='true' and/or THANOS_SKIP_TENCENT_COS_TESTS='true' if you want to skip e2e tests against real store buckets"
	THANOS_TEST_PROMETHEUS_VERSIONS="$(PROM_VERSIONS)" THANOS_TEST_ALERTMANAGER_PATH="alertmanager-$(ALERTMANAGER_VERSION)" go test $(shell go list ./... | grep -v /vendor/ | grep -v /benchmark/);

# test-deps installs dependency for e2e tets.
# It installs current Thanos, supported versions of Prometheus and alertmanager to test against in e2e.
.PHONY: test-deps
test-deps:
	@go install github.com/improbable-eng/thanos/cmd/thanos
	$(foreach ver,$(PROM_VERSIONS),$(call fetch_go_bin_version,github.com/prometheus/prometheus/cmd/prometheus,$(ver)))
	$(call fetch_go_bin_version,github.com/prometheus/alertmanager/cmd/alertmanager,$(ALERTMANAGER_VERSION))
	$(call fetch_go_bin_version,github.com/minio/minio,$(MINIO_SERVER_VERSION))

# vet vets the code.
.PHONY: vet
vet: check-git
	@echo ">> vetting code"
	@go vet ./...

# go mod related
.PHONY: go-mod-tidy
go-mod-tidy: check-git
	@go mod tidy

.PHONY: check-go-mod
check-go-mod:
	@go mod verify

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

.PHONY: web-serve
web-serve: web-pre-process $(HUGO)
	@echo ">> serving documentation website"
	@cd $(WEB_DIR) && $(HUGO) --config hugo.yaml -v server

# non-phony targets
$(EMBEDMD):
	$(call fetch_go_bin_version,github.com/campoy/embedmd,$(EMBEDMD_VERSION))

$(ERRCHECK):
	$(call fetch_go_bin_version,github.com/kisielk/errcheck,$(ERRCHECK_VERSION))

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

$(PROTOC):
	@mkdir -p $(TMP_GOPATH)
	@echo ">> fetching protoc@${PROTOC_VERSION}"
	@PROTOC_VERSION="$(PROTOC_VERSION)" TMP_GOPATH="$(TMP_GOPATH)" scripts/installprotoc.sh
	@echo ">> installing protoc@${PROTOC_VERSION}"
	@mv -- "$(TMP_GOPATH)/bin/protoc" "$(GOBIN)/protoc-$(PROTOC_VERSION)"
	@echo ">> produced $(GOBIN)/protoc-$(PROTOC_VERSION)"
