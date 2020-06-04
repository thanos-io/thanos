# Auto generated binary variables helper managed by https://github.com/bwplotka/bingo v0.1.0.rc.4. DO NOT EDIT.
# All tools are designed to be build inside $GOBIN.
GOBIN ?= $(firstword $(subst :, ,${GOPATH}))/bin
GO    ?= $(shell which go)

# Bellow generated variables ensure that every time a tool under each variable is invoked, the correct version
# will be used; reinstalling only if needed.
# For example for alertmanager variable:
#
# In your main Makefile (for non array binaries):
#
#include .bingo/Variables.mk # (If not generated automatically by bingo).
#
#command: $(ALERTMANAGER)
#	@echo "Running alertmanager"
#	@$(ALERTMANAGER) <flags/args..>
#
ALERTMANAGER ?= $(GOBIN)/alertmanager-v0.20.0
$(ALERTMANAGER): .bingo/alertmanager.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/alertmanager-v0.20.0"
	@$(GO) build -modfile=.bingo/alertmanager.mod -o=$(GOBIN)/alertmanager-v0.20.0 "github.com/prometheus/alertmanager/cmd/alertmanager"
.bingo/alertmanager.mod: ;

EMBEDMD ?= $(GOBIN)/embedmd-v0.0.0-20181127031020-97c13d6e4160
$(EMBEDMD): .bingo/embedmd.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/embedmd-v0.0.0-20181127031020-97c13d6e4160"
	@$(GO) build -modfile=.bingo/embedmd.mod -o=$(GOBIN)/embedmd-v0.0.0-20181127031020-97c13d6e4160 "github.com/campoy/embedmd"
.bingo/embedmd.mod: ;

FAILLINT ?= $(GOBIN)/faillint-v1.5.0
$(FAILLINT): .bingo/faillint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/faillint-v1.5.0"
	@$(GO) build -modfile=.bingo/faillint.mod -o=$(GOBIN)/faillint-v1.5.0 "github.com/fatih/faillint"
.bingo/faillint.mod: ;

GO_BINDATA ?= $(GOBIN)/go-bindata-v3.1.1+incompatible
$(GO_BINDATA): .bingo/go-bindata.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/go-bindata-v3.1.1+incompatible"
	@$(GO) build -modfile=.bingo/go-bindata.mod -o=$(GOBIN)/go-bindata-v3.1.1+incompatible "github.com/go-bindata/go-bindata/go-bindata"
.bingo/go-bindata.mod: ;

GOIMPORTS ?= $(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2
$(GOIMPORTS): .bingo/goimports.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2"
	@$(GO) build -modfile=.bingo/goimports.mod -o=$(GOBIN)/goimports-v0.0.0-20200526224456-8b020aee10d2 "golang.org/x/tools/cmd/goimports"
.bingo/goimports.mod: ;

GOJSONTOYAML ?= $(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742
$(GOJSONTOYAML): .bingo/gojsontoyaml.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742"
	@$(GO) build -modfile=.bingo/gojsontoyaml.mod -o=$(GOBIN)/gojsontoyaml-v0.0.0-20191212081931-bf2969bbd742 "github.com/brancz/gojsontoyaml"
.bingo/gojsontoyaml.mod: ;

GOLANGCI_LINT ?= $(GOBIN)/golangci-lint-v1.27.0
$(GOLANGCI_LINT): .bingo/golangci-lint.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/golangci-lint-v1.27.0"
	@$(GO) build -modfile=.bingo/golangci-lint.mod -o=$(GOBIN)/golangci-lint-v1.27.0 "github.com/golangci/golangci-lint/cmd/golangci-lint"
.bingo/golangci-lint.mod: ;

HUGO ?= $(GOBIN)/hugo-v0.55.3
$(HUGO): .bingo/hugo.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/hugo-v0.55.3"
	@$(GO) build -modfile=.bingo/hugo.mod -o=$(GOBIN)/hugo-v0.55.3 "github.com/gohugoio/hugo"
.bingo/hugo.mod: ;

JB ?= $(GOBIN)/jb-v0.2.1-0.20200211220001-efe0c9e86443
$(JB): .bingo/jb.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jb-v0.2.1-0.20200211220001-efe0c9e86443"
	@$(GO) build -modfile=.bingo/jb.mod -o=$(GOBIN)/jb-v0.2.1-0.20200211220001-efe0c9e86443 "github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb"
.bingo/jb.mod: ;

JSONNET ?= $(GOBIN)/jsonnet-v0.16.0
$(JSONNET): .bingo/jsonnet.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnet-v0.16.0"
	@$(GO) build -modfile=.bingo/jsonnet.mod -o=$(GOBIN)/jsonnet-v0.16.0 "github.com/google/go-jsonnet/cmd/jsonnet"
.bingo/jsonnet.mod: ;

JSONNETFMT ?= $(GOBIN)/jsonnetfmt-v0.16.0
$(JSONNETFMT): .bingo/jsonnetfmt.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/jsonnetfmt-v0.16.0"
	@$(GO) build -modfile=.bingo/jsonnetfmt.mod -o=$(GOBIN)/jsonnetfmt-v0.16.0 "github.com/google/go-jsonnet/cmd/jsonnetfmt"
.bingo/jsonnetfmt.mod: ;

LICHE ?= $(GOBIN)/liche-v0.0.0-20181124191719-2a2e6e56f6c6
$(LICHE): .bingo/liche.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/liche-v0.0.0-20181124191719-2a2e6e56f6c6"
	@$(GO) build -modfile=.bingo/liche.mod -o=$(GOBIN)/liche-v0.0.0-20181124191719-2a2e6e56f6c6 "github.com/raviqqe/liche"
.bingo/liche.mod: ;

MINIO ?= $(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da
$(MINIO): .bingo/minio.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da"
	@$(GO) build -modfile=.bingo/minio.mod -o=$(GOBIN)/minio-v0.0.0-20200527010300-cccf2de129da "github.com/minio/minio"
.bingo/minio.mod: ;

MISSPELL ?= $(GOBIN)/misspell-v0.3.4
$(MISSPELL): .bingo/misspell.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/misspell-v0.3.4"
	@$(GO) build -modfile=.bingo/misspell.mod -o=$(GOBIN)/misspell-v0.3.4 "github.com/client9/misspell/cmd/misspell"
.bingo/misspell.mod: ;

PROMETHEUS_ARRAY ?= $(GOBIN)/prometheus-v1.8.2-0.20200507164740-ecee9c8abfd1 $(GOBIN)/prometheus-v2.4.3+incompatible
$(PROMETHEUS_ARRAY): .bingo/prometheus.1.mod .bingo/prometheus.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/prometheus-v1.8.2-0.20200507164740-ecee9c8abfd1"
	@$(GO) build -modfile=.bingo/prometheus.1.mod -o=$(GOBIN)/prometheus-v1.8.2-0.20200507164740-ecee9c8abfd1 "github.com/prometheus/prometheus/cmd/prometheus"
	@echo "(re)installing $(GOBIN)/prometheus-v2.4.3+incompatible"
	@$(GO) build -modfile=.bingo/prometheus.mod -o=$(GOBIN)/prometheus-v2.4.3+incompatible "github.com/prometheus/prometheus/cmd/prometheus"
.bingo/prometheus.1.mod: ;
.bingo/prometheus.mod: ;

PROMTOOL ?= $(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05
$(PROMTOOL): .bingo/promtool.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05"
	@$(GO) build -modfile=.bingo/promtool.mod -o=$(GOBIN)/promtool-v1.8.2-0.20200522113006-f4dd45609a05 "github.com/prometheus/prometheus/cmd/promtool"
.bingo/promtool.mod: ;

PROMU ?= $(GOBIN)/promu-v0.5.0
$(PROMU): .bingo/promu.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/promu-v0.5.0"
	@$(GO) build -modfile=.bingo/promu.mod -o=$(GOBIN)/promu-v0.5.0 "github.com/prometheus/promu"
.bingo/promu.mod: ;

