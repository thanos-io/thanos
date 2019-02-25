GOPATH      ?= $(shell go env GOPATH)
GOIMPORTS   ?= $(GOPATH)/bin/goimports
GO111MODULE ?= on

.PHONY: all
all: deps build

.PHONY: deps
deps:
	@echo ">> fetching dependencies"
	@GO111MODULE=$(GO111MODULE) go mod vendor

.PHONY: build
build:
	@echo ">> building thanosbench"
	@go install github.com/improbable-eng/thanos/benchmark/cmd/thanosbench

format: $(GOIMPORTS) deps
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")

$(GOIMPORTS): 
	@echo ">> fetching goimports"
	@go get -u golang.org/x/tools/cmd/goimports
