PREFIX ?= $(shell pwd)

all: install-tools format build

format:
	@echo ">> formatting code"
	@goimports -w ./ $(go list ./... | grep -v /vendor/)

vet:
	@echo ">> vetting code"
	@go vet ./...

build:
	@echo ">> building binaries"
	@promu build --prefix $(PREFIX)

install-tools:
	@echo ">> fetching goimports"
	@go get -u golang.org/x/tools/cmd/goimports
	@echo ">> fetching promu"
	@go get -u github.com/prometheus/promu

proto:
	@go get -u github.com/gogo/protobuf/protoc-gen-gogofast
	@./scripts/genproto.sh

test:
	@echo ">> running all tests"
	@go test $(shell go list ./... | grep -v /vendor/)

.PHONY: all install-tools format vet build promu