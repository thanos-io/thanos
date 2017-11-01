PREFIX ?= $(shell pwd)

all: format build

format:
	@echo ">> formatting code"
	@go fmt ./...

vet:
	@echo ">> vetting code"
	@go vet ./...

build: promu
	@echo ">> building binaries"
	@promu build --prefix $(PREFIX)

promu:
	@echo ">> fetching promu"
	@go get -u github.com/prometheus/promu


.PHONY: all format vet build promu