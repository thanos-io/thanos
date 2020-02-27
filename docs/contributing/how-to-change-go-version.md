---
title: Changing Golang version
type: docs
menu: contributing
slug: /how-to-change-go-version.md
---

Thanos build system is pinned to certain Golang version. This is to ensure that Golang version
changes is done by us in controlled, traceable way.

To update Thanos build system to newer Golang:

1. Edit [.promu.yaml](/.promu.yml) and edit `go: version: <go version>` in YAML to desired version. This will ensure that all artifacts are
  built with desired Golang version. How to verify? Download tarball, unpack and invoke `thanos --version`
1. Edit [.circleci/config.yaml](/.circleci/config.yml) and update ` - image: circleci/golang:<go version>` to desired
  Golang version. This will ensure that all docker images and go tests are using desired Golang version. How to verify? Invoke `docker pull quay.io/thanos/thanos:<version> --version`
1. Edit [.Dockerfile.thanos-ci](/Dockerfile.thanos-ci) and update Go version. Run `make docker-ci DOCKER_CI_TAG=<new tag>`. Update [.circleci/config.yaml](/.circleci/config.yml) thanos-ci image to `<new tag>`.
1. Edit [.github/workflows/e2e.yaml](/.github/workflows/e2e.yaml), [.github/workflows/cross-build.yaml](/.github/workflows/cross-build.yaml) and update Go version.
