PREFIX            ?= $(shell pwd)
FILES             ?= $(shell find . -type f -name '*.go' -not -path "./vendor/*")
DOCKER_IMAGE_NAME ?= thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)
# $GOPATH/bin might not be in $PATH, so we can't assume `which` would give use
# the path of promu et al. As for selecting the first GOPATH, we assume:
# - most people only have one GOPATH at a time;
# - if you don't have one or any of those tools installed, running `go get`
#   would place them in the first GOPATH.
# It's possible that any of the tools would be installed in the other GOPATHs,
# but for simplicity sake we just make sure they exist in the first one, and
# then keep using those.
FIRST_GOPATH      ?= $(firstword $(subst :, ,$(shell go env GOPATH)))
TMP_GOPATH        ?= /tmp/thanos-go
BIN_DIR           ?= $(FIRST_GOPATH)/bin
GO111MODULE       ?= on
export GO111MODULE

# Tools.
EMBEDMD           ?= $(BIN_DIR)/embedmd-$(EMBEDMD_VERSION)
# v2.0.0
EMBEDMD_VERSION   ?= 97c13d6e41602fc6e397eb51c45f38069371a969
ERRCHECK          ?= $(BIN_DIR)/errcheck-$(ERRCHECK_VERSION)
# v1.2.0
ERRCHECK_VERSION  ?= e14f8d59a22d460d56c5ee92507cd94c78fbf274
LICHE             ?= $(BIN_DIR)/liche-$(LICHE_VERSION)
LICHE_VERSION     ?= 2a2e6e56f6c615c17b2e116669c4cdb31b5453f3
GOIMPORTS         ?= $(BIN_DIR)/goimports-$(GOIMPORTS_VERSION)
GOIMPORTS_VERSION ?= 1c3d964395ce8f04f3b03b30aaed0b096c08c3c6
PROMU             ?= $(BIN_DIR)/promu-$(PROMU_VERSION)
# v0.2.0
PROMU_VERSION     ?= 264dc36af9ea3103255063497636bd5713e3e9c1
PROTOC            ?= $(BIN_DIR)/protoc-$(PROTOC_VERSION)
PROTOC_VERSION    ?= 3.4.0
GIT               ?= $(shell which git)
BZR               ?= $(shell which bzr)

# E2e test deps.
# Referenced by github.com/improbable-eng/thanos/blob/master/docs/getting_started.md#prometheus

# Limitied prom version, because testing was not possibe. This should fix it: https://github.com/improbable-eng/thanos/issues/758
PROM_VERSIONS           ?=v2.4.3 v2.5.0
ALERTMANAGER_VERSION    ?=v0.15.2
MINIO_SERVER_VERSION    ?=RELEASE.2018-10-06T00-15-16Z

# fetch_go_bin_version downloads (go gets) the binary from specific version and installs it in $(BIN_DIR)/<bin>-<version>
# arguments:
# $(1): Install path. (e.g github.com/campoy/embedmd)
# $(2): Tag or revision for checkout.
define fetch_go_bin_version
	@mkdir -p $(BIN_DIR)

	@echo ">> fetching $(1)@$(2) revision/version"
	@if [ ! -d '$(TMP_GOPATH)/src/$(1)' ]; then \
    GOPATH='$(TMP_GOPATH)' GO111MODULE='off' go get -d -u '$(1)/...'; \
  else \
    CDPATH='' cd -- '$(TMP_GOPATH)/src/$(1)' && git fetch; \
  fi
	@CDPATH='' cd -- '$(TMP_GOPATH)/src/$(1)' && git checkout -f -q '$(2)'
	@echo ">> installing $(1)@$(2)"
	@GOBIN='$(TMP_GOPATH)/bin' GOPATH='$(TMP_GOPATH)' GO111MODULE='off' go install '$(1)'
	@mv -- '$(TMP_GOPATH)/bin/$(shell basename $(1))' '$(BIN_DIR)/$(shell basename $(1))-$(2)'
	@echo ">> produced $(BIN_DIR)/$(shell basename $(1))-$(2)"

endef

.PHONY: all
all: format build

# assets repacks all statis assets into go file for easier deploy.
.PHONY: assets
assets:
	@echo ">> deleting asset file"
	@rm pkg/ui/bindata.go || true
	@echo ">> writing assets"
	@go get -u github.com/jteeuwen/go-bindata/...
	@go-bindata $(bindata_flags) -pkg ui -o pkg/ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  pkg/ui/templates/... pkg/ui/static/...
	@go fmt ./pkg/ui


# build builds Thanos binary using `promu`.
.PHONY: build
build: check-git check-bzr go-mod-tidy $(PROMU)
	@echo ">> building binaries"
	@$(PROMU) build --prefix $(PREFIX)

# crossbuild builds all binaries for all platforms.
.PHONY: crossbuild
crossbuild: check-git check-bzr go-mod-tidy $(PROMU)
	@echo ">> crossbuilding all binaries"
	$(PROMU) crossbuild -v

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
	@$(LICHE) --recursive docs --document-root .

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
	@$(GOIMPORTS) -w $(FILES)

# proto generates golang files from Thanos proto files.
.PHONY: proto
proto: check-git check-bzr $(GOIMPORTS) $(PROTOC)
	@go install ./vendor/github.com/gogo/protobuf/protoc-gen-gogofast
	@GOIMPORTS_BIN="$(GOIMPORTS)" PROTOC_BIN="$(PROTOC)" scripts/genproto.sh

.PHONY: promu
promu: $(PROMU)

# tarball builds release tarball.
.PHONY: tarball
tarball: $(PROMU)
	@echo ">> building release tarball"
	$(PROMU) tarball --prefix $(PREFIX) $(BIN_DIR)

.PHONY: tarballs-release
tarballs-release: $(PROMU)
	@echo ">> Publishing tarballs"
	$(PROMU) crossbuild tarballs
	$(PROMU) checksum .tarballs
	$(PROMU) release .tarballs

# test runs all Thanos golang tests against each supported version of Prometheus.
.PHONY: test
test: check-git check-bzr test-deps
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
vet: check-git check-bzr
	@echo ">> vetting code"
	@go vet ./...

# go mod related
.PHONY: go-mod-tidy
go-mod-tidy: check-git check-bzr
	@go mod tidy

.PHONY: check-go-mod
check-go-mod: go-mod-tidy
	@git diff --exit-code go.mod go.sum > /dev/null || echo >&2 "go.mod and/or go.sum have uncommited changes. See CONTRIBUTING.md."

# tooling deps. TODO(bwplotka): Pin them all to certain version!
.PHONY: check-git
check-git:
ifneq ($(GIT),)
	@test -x $(GIT) || (echo >&2 "No git executable binary found at $(GIT)."; exit 1)
else
	@echo >&2 "No git binary found."; exit 1
endif

.PHONY: check-bzr
check-bzr:
ifneq ($(BZR),)
	@test -x $(BZR) || (echo >&2 "No bzr exectuable binary found at $(BZR)."; exit 1)
else
	@echo >&2 "No bzr binary found."; exit 1
endif

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

$(PROTOC):
	@mkdir -p $(TMP_GOPATH)
	@echo ">> fetching protoc@${PROTOC_VERSION}"
	@PROTOC_VERSION="$(PROTOC_VERSION)" TMP_GOPATH="$(TMP_GOPATH)" scripts/installprotoc.sh
	@echo ">> installing protoc@${PROTOC_VERSION}"
	@mv -- "$(TMP_GOPATH)/bin/protoc" "$(BIN_DIR)/protoc-$(PROTOC_VERSION)"
	@echo ">> produced $(BIN_DIR)/protoc-$(PROTOC_VERSION)"
