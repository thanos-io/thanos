# Auto generated binary variables helper managed by https://github.com/bwplotka/bingo v0.2.2. DO NOT EDIT.
# All tools are designed to be build inside $GOBIN.
GOPATH ?= $(shell go env GOPATH)
GOBIN  ?= $(firstword $(subst :, ,${GOPATH}))/bin
GO     ?= $(shell which go)

# Bellow generated variables ensure that every time a tool under each variable is invoked, the correct version
# will be used; reinstalling only if needed.
# For example for alertmanager variable:
#
# In your main Makefile (for non array binaries):
#
#include .bingo/Variables.mk # Assuming -dir was set to .bingo .
#
#command: $(ALERTMANAGER)
#	@echo "Running alertmanager"
#	@$(ALERTMANAGER) <flags/args..>
#
ALERTMANAGER := $(GOBIN)/alertmanager-v0.20.0
$(ALERTMANAGER): .bingo/alertmanager.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/alertmanager-v0.20.0"
	@cd .bingo && $(GO) build -modfile=alertmanager.mod -o=$(GOBIN)/alertmanager-v0.20.0 "github.com/prometheus/alertmanager/cmd/alertmanager"

BINGO := $(GOBIN)/bingo-v0.2.2
$(BINGO): .bingo/bingo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/bingo-v0.2.2"
	@cd .bingo && $(GO) build -modfile=bingo.mod -o=$(GOBIN)/bingo-v0.2.2 "github.com/bwplotka/bingo"

EMBEDMD := $(GOBIN)/embedmd-v0.0.0-20181127031020-97c13d6e4160
$(EMBEDMD): .bingo/embedmd.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/embedmd-v0.0.0-20181127031020-97c13d6e4160"
	@cd .bingo && $(GO) build -modfile=embedmd.mod -o=$(GOBIN)/embedmd-v0.0.0-20181127031020-97c13d6e4160 "github.com/campoy/embedmd"

FAILLINT := $(GOBIN)/faillint-v1.5.0
$(FAILLINT): .bingo/faillint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/faillint-v1.5.0"
	@cd .bingo && $(GO) build -modfile=faillint.mod -o=$(GOBIN)/faillint-v1.5.0 "github.com/fatih/faillint"

GO_BINDATA := $(GOBIN)/go-bindata-v3.1.1+incompatible
$(GO_BINDATA): .bingo/go-bindata.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/go-bindata-v3.1.1+incompatible"
	@cd .bingo && $(GO) build -modfile=go-bindata.mod -o=$(GOBIN)/go-bindata-v3.1.1+incompatible "github.com/go-bindata/go-bindata/go-bindata"

GOIMPORTS := $(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2
$(GOIMPORTS): .bingo/goimports.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2"
	@cd .bingo && $(GO) build -modfile=goimports.mod -o=$(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2 "golang.org/x/tools/cmd/goimports"

GOJSONTOYAML := $(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742
$(GOJSONTOYAML): .bingo/gojsontoyaml.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742"
	@cd .bingo && $(GO) build -modfile=gojsontoyaml.mod -o=$(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742 "github.com/brancz/gojsontoyaml"

GOLANGCI_LINT := $(GOBIN)/golangci-lint-v1.29.0
$(GOLANGCI_LINT): .bingo/golangci-lint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/golangci-lint-v1.29.0"
	@cd .bingo && $(GO) build -modfile=golangci-lint.mod -o=$(GOBIN)/golangci-lint-v1.29.0 "github.com/golangci/golangci-lint/cmd/golangci-lint"

HUGO := $(GOBIN)/hugo-v0.74.3
$(HUGO): .bingo/hugo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/hugo-v0.74.3"
	@cd .bingo && $(GO) build -modfile=hugo.mod -o=$(GOBIN)/hugo-v0.74.3 "github.com/gohugoio/hugo"

JB := $(GOBIN)/jb-v0.4.0
$(JB): .bingo/jb.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jb-v0.4.0"
	@cd .bingo && $(GO) build -modfile=jb.mod -o=$(GOBIN)/jb-v0.4.0 "github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb"

JSONNET := $(GOBIN)/jsonnet-v0.16.0
$(JSONNET): .bingo/jsonnet.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnet-v0.16.0"
	@cd .bingo && $(GO) build -modfile=jsonnet.mod -o=$(GOBIN)/jsonnet-v0.16.0 "github.com/google/go-jsonnet/cmd/jsonnet"

JSONNETFMT := $(GOBIN)/jsonnetfmt-v0.16.0
$(JSONNETFMT): .bingo/jsonnetfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnetfmt-v0.16.0"
	@cd .bingo && $(GO) build -modfile=jsonnetfmt.mod -o=$(GOBIN)/jsonnetfmt-v0.16.0 "github.com/google/go-jsonnet/cmd/jsonnetfmt"

MINIO := $(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da
$(MINIO): .bingo/minio.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da"
	@cd .bingo && $(GO) build -modfile=minio.mod -o=$(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da "github.com/minio/minio"

PROMETHEUS_ARRAY := $(GOBIN)/prometheus-v2.4.3+incompatible $(GOBIN)/prometheus-v1.8.2-0.20200724121523-657ba532e42f
$(PROMETHEUS_ARRAY): .bingo/prometheus.mod .bingo/prometheus.1.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/prometheus-v2.4.3+incompatible"
	@cd .bingo && $(GO) build -modfile=prometheus.mod -o=$(GOBIN)/prometheus-v2.4.3+incompatible "github.com/prometheus/prometheus/cmd/prometheus"
	@echo "(re)installing $(GOBIN)/prometheus-v1.8.2-0.20200724121523-657ba532e42f"
	@cd .bingo && $(GO) build -modfile=prometheus.1.mod -o=$(GOBIN)/prometheus-v1.8.2-0.20200724121523-657ba532e42f "github.com/prometheus/prometheus/cmd/prometheus"

PROMTOOL := $(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05
$(PROMTOOL): .bingo/promtool.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05"
	@cd .bingo && $(GO) build -modfile=promtool.mod -o=$(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05 "github.com/prometheus/prometheus/cmd/promtool"

PROMU := $(GOBIN)/promu-v0.5.0
$(PROMU): .bingo/promu.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promu-v0.5.0"
	@cd .bingo && $(GO) build -modfile=promu.mod -o=$(GOBIN)/promu-v0.5.0 "github.com/prometheus/promu"

PROTOC_GEN_GOGOFAST := $(GOBIN)/protoc-gen-gogofast-v1.3.1
$(PROTOC_GEN_GOGOFAST): .bingo/protoc-gen-gogofast.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-gen-gogofast-v1.3.1"
	@cd .bingo && $(GO) build -modfile=protoc-gen-gogofast.mod -o=$(GOBIN)/protoc-gen-gogofast-v1.3.1 "github.com/gogo/protobuf/protoc-gen-gogofast"

SHFMT := $(GOBIN)/shfmt-v3.1.2
$(SHFMT): .bingo/shfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/shfmt-v3.1.2"
	@cd .bingo && $(GO) build -modfile=shfmt.mod -o=$(GOBIN)/shfmt-v3.1.2 "mvdan.cc/sh/v3/cmd/shfmt"

