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
1. Edit [.circleci/config.yaml](/.circleci/config.yml) and edit ` - image: circleci/golang:<go version>` to desired 
  Golang version. This will ensure that all docker images and go tests are using desired Golang version. How to verify? Invoke `docker pull improbable/thanos:<version> --version`