# Auto generated binary variables helper managed by https://github.com/bwplotka/bingo v0.9. DO NOT EDIT.
# All tools are designed to be build inside $GOBIN.
BINGO_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
GOPATH ?= $(shell go env GOPATH)
GOBIN  ?= $(firstword $(subst :, ,${GOPATH}))/bin
GO     ?= $(shell which go)

# Below generated variables ensure that every time a tool under each variable is invoked, the correct version
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
ALERTMANAGER := $(GOBIN)/alertmanager-v0.24.0
$(ALERTMANAGER): $(BINGO_DIR)/alertmanager.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/alertmanager-v0.24.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=alertmanager.mod -o=$(GOBIN)/alertmanager-v0.24.0 "github.com/prometheus/alertmanager/cmd/alertmanager"

BINGO := $(GOBIN)/bingo-v0.9.0
$(BINGO): $(BINGO_DIR)/bingo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/bingo-v0.9.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=bingo.mod -o=$(GOBIN)/bingo-v0.9.0 "github.com/bwplotka/bingo"

FAILLINT := $(GOBIN)/faillint-v1.13.0
$(FAILLINT): $(BINGO_DIR)/faillint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/faillint-v1.13.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=faillint.mod -o=$(GOBIN)/faillint-v1.13.0 "github.com/fatih/faillint"

GOIMPORTS := $(GOBIN)/goimports-v0.23.0
$(GOIMPORTS): $(BINGO_DIR)/goimports.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/goimports-v0.23.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=goimports.mod -o=$(GOBIN)/goimports-v0.23.0 "golang.org/x/tools/cmd/goimports"

GOJSONTOYAML := $(GOBIN)/gojsontoyaml-v0.1.0
$(GOJSONTOYAML): $(BINGO_DIR)/gojsontoyaml.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/gojsontoyaml-v0.1.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=gojsontoyaml.mod -o=$(GOBIN)/gojsontoyaml-v0.1.0 "github.com/brancz/gojsontoyaml"

GOLANGCI_LINT := $(GOBIN)/golangci-lint-v1.59.1
$(GOLANGCI_LINT): $(BINGO_DIR)/golangci-lint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/golangci-lint-v1.59.1"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=golangci-lint.mod -o=$(GOBIN)/golangci-lint-v1.59.1 "github.com/golangci/golangci-lint/cmd/golangci-lint"

GOTESPLIT := $(GOBIN)/gotesplit-v0.2.1
$(GOTESPLIT): $(BINGO_DIR)/gotesplit.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/gotesplit-v0.2.1"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=gotesplit.mod -o=$(GOBIN)/gotesplit-v0.2.1 "github.com/Songmu/gotesplit/cmd/gotesplit"

HUGO := $(GOBIN)/hugo-v0.101.0
$(HUGO): $(BINGO_DIR)/hugo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/hugo-v0.101.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=hugo.mod -o=$(GOBIN)/hugo-v0.101.0 "github.com/gohugoio/hugo"

JB := $(GOBIN)/jb-v0.5.1
$(JB): $(BINGO_DIR)/jb.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jb-v0.5.1"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=jb.mod -o=$(GOBIN)/jb-v0.5.1 "github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb"

JSONNET_LINT := $(GOBIN)/jsonnet-lint-v0.18.0
$(JSONNET_LINT): $(BINGO_DIR)/jsonnet-lint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnet-lint-v0.18.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=jsonnet-lint.mod -o=$(GOBIN)/jsonnet-lint-v0.18.0 "github.com/google/go-jsonnet/cmd/jsonnet-lint"

JSONNET := $(GOBIN)/jsonnet-v0.18.0
$(JSONNET): $(BINGO_DIR)/jsonnet.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnet-v0.18.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=jsonnet.mod -o=$(GOBIN)/jsonnet-v0.18.0 "github.com/google/go-jsonnet/cmd/jsonnet"

JSONNETFMT := $(GOBIN)/jsonnetfmt-v0.18.0
$(JSONNETFMT): $(BINGO_DIR)/jsonnetfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnetfmt-v0.18.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=jsonnetfmt.mod -o=$(GOBIN)/jsonnetfmt-v0.18.0 "github.com/google/go-jsonnet/cmd/jsonnetfmt"

MDOX := $(GOBIN)/mdox-v0.9.1-0.20220713110358-25b9abcf90a0
$(MDOX): $(BINGO_DIR)/mdox.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/mdox-v0.9.1-0.20220713110358-25b9abcf90a0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=mdox.mod -o=$(GOBIN)/mdox-v0.9.1-0.20220713110358-25b9abcf90a0 "github.com/bwplotka/mdox"

MINIO := $(GOBIN)/minio-v0.0.0-20220720015624-ce8397f7d944
$(MINIO): $(BINGO_DIR)/minio.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/minio-v0.0.0-20220720015624-ce8397f7d944"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=minio.mod -o=$(GOBIN)/minio-v0.0.0-20220720015624-ce8397f7d944 "github.com/minio/minio"

PROMDOC := $(GOBIN)/promdoc-v0.8.0
$(PROMDOC): $(BINGO_DIR)/promdoc.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promdoc-v0.8.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=promdoc.mod -o=$(GOBIN)/promdoc-v0.8.0 "github.com/plexsystems/promdoc"

PROMETHEUS := $(GOBIN)/prometheus-v0.37.0
$(PROMETHEUS): $(BINGO_DIR)/prometheus.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/prometheus-v0.37.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=prometheus.mod -o=$(GOBIN)/prometheus-v0.37.0 "github.com/prometheus/prometheus/cmd/prometheus"

PROMTOOL := $(GOBIN)/promtool-v0.47.0
$(PROMTOOL): $(BINGO_DIR)/promtool.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promtool-v0.47.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=promtool.mod -o=$(GOBIN)/promtool-v0.47.0 "github.com/prometheus/prometheus/cmd/promtool"

PROMU := $(GOBIN)/promu-v0.5.0
$(PROMU): $(BINGO_DIR)/promu.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promu-v0.5.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=promu.mod -o=$(GOBIN)/promu-v0.5.0 "github.com/prometheus/promu"

PROTOC_GEN_GO_GRPC := $(GOBIN)/protoc-gen-go-grpc-v1.5.1
$(PROTOC_GEN_GO_GRPC): $(BINGO_DIR)/protoc-gen-go-grpc.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-gen-go-grpc-v1.5.1"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=protoc-gen-go-grpc.mod -o=$(GOBIN)/protoc-gen-go-grpc-v1.5.1 "google.golang.org/grpc/cmd/protoc-gen-go-grpc"

PROTOC_GEN_GO_VTPROTO := $(GOBIN)/protoc-gen-go-vtproto-v0.6.1-0.20240319094008-0393e58bdf10
$(PROTOC_GEN_GO_VTPROTO): $(BINGO_DIR)/protoc-gen-go-vtproto.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-gen-go-vtproto-v0.6.1-0.20240319094008-0393e58bdf10"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=protoc-gen-go-vtproto.mod -o=$(GOBIN)/protoc-gen-go-vtproto-v0.6.1-0.20240319094008-0393e58bdf10 "github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto"

PROTOC_GEN_GO := $(GOBIN)/protoc-gen-go-v1.34.2
$(PROTOC_GEN_GO): $(BINGO_DIR)/protoc-gen-go.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-gen-go-v1.34.2"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=protoc-gen-go.mod -o=$(GOBIN)/protoc-gen-go-v1.34.2 "google.golang.org/protobuf/cmd/protoc-gen-go"

PROTOC_GEN_GOGOFAST := $(GOBIN)/protoc-gen-gogofast-v1.3.2
$(PROTOC_GEN_GOGOFAST): $(BINGO_DIR)/protoc-gen-gogofast.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-gen-gogofast-v1.3.2"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=protoc-gen-gogofast.mod -o=$(GOBIN)/protoc-gen-gogofast-v1.3.2 "github.com/gogo/protobuf/protoc-gen-gogofast"

PROTOC_GO_INJECT_TAG := $(GOBIN)/protoc-go-inject-tag-v1.4.0
$(PROTOC_GO_INJECT_TAG): $(BINGO_DIR)/protoc-go-inject-tag.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/protoc-go-inject-tag-v1.4.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=protoc-go-inject-tag.mod -o=$(GOBIN)/protoc-go-inject-tag-v1.4.0 "github.com/favadi/protoc-go-inject-tag"

SHFMT := $(GOBIN)/shfmt-v3.8.0
$(SHFMT): $(BINGO_DIR)/shfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/shfmt-v3.8.0"
	@cd $(BINGO_DIR) && GOWORK=off $(GO) build -mod=mod -modfile=shfmt.mod -o=$(GOBIN)/shfmt-v3.8.0 "mvdan.cc/sh/v3/cmd/shfmt"

