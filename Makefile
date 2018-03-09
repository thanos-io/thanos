PREFIX            ?= $(shell pwd)
FILES             ?= $(shell find . -type f -name '*.go' -not -path "./vendor/*")
DOCKER_IMAGE_NAME ?= thanos
DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))

all: install-tools deps format build

deps:
	@echo ">> dep ensure"
	@dep ensure

format:
	@echo ">> formatting code"
	@goimports -w $(FILES)

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
	@echo ">> fetching dep"
	@go get -u github.com/golang/dep/cmd/dep

test-deps:
	@go install github.com/improbable-eng/thanos/cmd/thanos
	@go get -u github.com/prometheus/prometheus/cmd/prometheus
	@go get -u github.com/prometheus/alertmanager/cmd/alertmanager

proto:
	@go get -u github.com/gogo/protobuf/protoc-gen-gogofast
	@./scripts/genproto.sh

test: test-deps
	@echo ">> running all tests"
	@go test $(shell go list ./... | grep -v /vendor/)

assets:
	@echo ">> writing assets"
	@go get -u github.com/jteeuwen/go-bindata/...
	@go-bindata $(bindata_flags) -pkg ui -o pkg/query/ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  pkg/query/ui/templates/... pkg/query/ui/static/...
	@go fmt ./pkg/query/ui

docker: build
	@echo ">> building docker image"
	@docker build -t improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)" .

docker-push:
	@echo ">> pushing image"
	@docker push improbable/"$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)"

docs:
	@go get -u github.com/campoy/embedmd
	@go build ./cmd/thanos/...
	@scripts/genflagdocs.sh

.PHONY: all install-tools format vet build assets docker docker-push docs deps
