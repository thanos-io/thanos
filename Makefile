PREFIX ?= $(shell pwd)
GO_FILES := $(go list ./... | grep -v /vendor/)

all: format build

format:
	@echo ">> formatting code"
	@goimports -w ./ $(GO_FILES)

vet:
	@echo ">> vetting code"
	@go vet ./...

build: promu
	@echo ">> building binaries"
	@promu build --prefix $(PREFIX)

promu:
	@echo ">> fetching promu"
	@go get -u github.com/prometheus/promu

proto:
	@go get -u github.com/gogo/protobuf/protoc-gen-gogofast
	@./scripts/genproto.sh

test:
	@echo ">> running all tests"
	@go test $(GO_FILES)

.PHONY: all format vet build promu