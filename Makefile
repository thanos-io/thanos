PREFIX            ?= $(shell pwd)
FILES             ?= $(shell find . -type f -name '*.go' -not -path "./vendor/*")
DOCKER_IMAGE_NAME ?= thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)
# $GOPATH/bin might not be in $PATH, so we can't assume `which` would give use
# the path of promu, dep et al. As for selecting the first GOPATH, we assume:
# - most people only have one GOPATH at a time;
# - if you don't have one or any of those tools installed, running `go get`
#   would place them in the first GOPATH.
# It's possible that any of the tools would be installed in the other GOPATHs,
# but for simplicity sake we just make sure they exist in the first one, and
# then keep using those.
FIRST_GOPATH      ?= $(firstword $(subst :, ,$(shell go env GOPATH)))
GOIMPORTS         ?= $(FIRST_GOPATH)/bin/goimports
PROMU             ?= $(FIRST_GOPATH)/bin/promu
DEP               ?= $(FIRST_GOPATH)/bin/dep
ERRCHECK          ?= $(FIRST_GOPATH)/bin/errcheck

.PHONY: all
all: deps format errcheck build

# assets repacks all statis assets into go file for easier deploy.
.PHONY: assets
assets:
	@echo ">> deleting asset file"
	@rm pkg/query/ui/bindata.go || true
	@echo ">> writing assets"
	@go get -u github.com/jteeuwen/go-bindata/...
	@go-bindata $(bindata_flags) -pkg ui -o pkg/query/ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  pkg/query/ui/templates/... pkg/query/ui/static/...
	@go fmt ./pkg/query/ui

# build builds Thanos binary using `promu`.
.PHONY: build
build: deps $(PROMU)
	@echo ">> building binaries"
	@$(PROMU) build --prefix $(PREFIX)

# crossbuild builds all binaries for all platforms.
.PHONY: crossbuild
crossbuild: deps $(PROMU)
	@echo ">> crossbuilding all binaries"
	$(PROMU) crossbuild -v

# deps fetches all necessary golang dependencies, since they are not checked into repository.
.PHONY: deps
deps: vendor

# docker builds docker with no tag.
.PHONY: docker
docker: build
	@echo ">> building docker image '${DOCKER_IMAGE_NAME}'"
	@docker build -t "${DOCKER_IMAGE_NAME}" .

# docker-push pushes docker image build under `${DOCKER_IMAGE_NAME}` to improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"
.PHONY: docker-push
docker-push:
	@echo ">> pushing image"
	@docker tag "${DOCKER_IMAGE_NAME}" improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"
	@docker push improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"

# docs regenerates flags in docs for all thanos commands.
.PHONY: docs
docs:
	@go get -u github.com/campoy/embedmd
	@go build ./cmd/thanos/...
	@scripts/genflagdocs.sh

# errcheck performs static analysis and returns error if any of the errors is not checked.
.PHONY: errcheck
errcheck: $(ERRCHECK)
	@echo ">> errchecking the code"
	$(ERRCHECK) -verbose -exclude .errcheck_excludes.txt ./...

# format formats the code (including imports format).
.PHONY: format
format: $(GOIMPORTS) deps
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(FILES)

# proto generates golang files from Thanos proto files.
.PHONY: proto
proto:
	@go get -u github.com/gogo/protobuf/protoc-gen-gogofast
	@./scripts/genproto.sh

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

# test runs all Thanos golang tests.
.PHONY: test
test: test-deps
	@echo ">> running all tests. Do export THANOS_SKIP_GCS_TESTS="true" or/and  export THANOS_SKIP_S3_AWS_TESTS="true" if you want to skip e2e tests against real store buckets"
	@go test $(shell go list ./... | grep -v /vendor/)


# test-deps installs dependency for e2e tets.
.PHONY: test-deps
test-deps: deps
	@go install github.com/improbable-eng/thanos/cmd/thanos
	@go get -u github.com/prometheus/prometheus/cmd/prometheus
	@go get -u github.com/prometheus/alertmanager/cmd/alertmanager

# vet vets the code.
.PHONY: vet
vet:
	@echo ">> vetting code"
	@go vet ./...

# non-phony targets

vendor: Gopkg.toml Gopkg.lock | $(DEP)
	@echo ">> dep ensure"
	@$(DEP) ensure

$(GOIMPORTS):
	@echo ">> fetching goimports"
	@go get -u golang.org/x/tools/cmd/goimports

$(PROMU):
	@echo ">> fetching promu"
	GOOS= GOARCH= go get -u github.com/prometheus/promu

$(DEP):
	@echo ">> fetching dep"
	@go get -u github.com/golang/dep/cmd/dep

$(ERRCHECK):
	@echo ">> fetching errcheck"
	@go get -u github.com/kisielk/errcheck
