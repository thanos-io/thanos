GOPATH    ?= $(shell go env GOPATH)
GOIMPORTS ?= $(GOPATH)/bin/goimports
DEP       ?= $(GOPATH)/bin/dep

.PHONY: all
all: deps build

.PHONY: deps
deps: $(DEP)
	@echo ">> fetching dependencies"
	@$(DEP) ensure

.PHONY: build
build:
	@echo ">> building thanosbench"
	@go install github.com/improbable-eng/thanos/benchmark/cmd/thanosbench

format: $(GOIMPORTS) deps
	@echo ">> formatting code"
	@$(GOIMPORTS) -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")

$(DEP):
	@echo ">> fetching dep"
	@go get -u github.com/golang/dep/cmd/dep

$(GOIMPORTS): 
	@echo ">> fetching goimports"
	@go get -u golang.org/x/tools/cmd/goimports
