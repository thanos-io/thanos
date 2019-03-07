GOPATH      ?= $(shell go env GOPATH)
GOIMPORTS   ?= $(GOPATH)/bin/goimports
GO111MODULE ?= on
export GO111MODULE

.PHONY: all
all: build

.PHONY: build
build:
	@echo ">> building thanosbench"
	@go install github.com/improbable-eng/thanos/benchmark/cmd/thanosbench

format: $(GOIMPORTS)
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")

$(GOIMPORTS): 
	@echo ">> fetching goimports"
	@go get -u golang.org/x/tools/cmd/goimports
