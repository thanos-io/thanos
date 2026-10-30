# Changing Golang version

The Thanos build system pins Go versions so that toolchain upgrades are controlled and traceable.

To update the Thanos build system to a newer Go version:

1. Update [`.go-version`](../../.go-version). The GitHub workflows in [`.github/workflows/docs.yaml`](../../.github/workflows/docs.yaml) and [`.github/workflows/go.yaml`](../../.github/workflows/go.yaml) use `go-version-file: .go-version`, so their Go version does not need to be edited separately.
2. Update the `go` directive in [`go.mod`](../../go.mod) and `go.version` in [`.promu.yml`](../../.promu.yml).
3. Update the `cimg/go:<go version>-node` images in [`.circleci/config.yml`](../../.circleci/config.yml).
4. Update the Go builder images in [`Dockerfile.e2e-tests`](../../Dockerfile.e2e-tests), [`Dockerfile.multi-stage`](../../Dockerfile.multi-stage), and [`cloudflare/thanos.dockerfile`](../../cloudflare/thanos.dockerfile). Update pinned image digests along with their tags.
5. Check the `cloudflare/ci/golang/make` component used by [`.gitlab-ci.yml`](../../.gitlab-ci.yml). Its builder image supplies the Go toolchain for the amd64 and arm64 package builds. These jobs run `make build PREFIX=output`; the subsequent nfpm jobs package `output/thanos` without recompiling it. The container Dockerfile's Go version does not control these package builds.
6. Update the development requirements in [`.bingo/README.md`](../../.bingo/README.md).

Verify the actual toolchain in the resulting artifacts, rather than relying only on configuration: run `thanos --version` or `go version -m /path/to/thanos` on the binary extracted from each release archive, package, or container image. The `go` directive specifies a minimum version; it does not pin the compiler to an exact patch version.
