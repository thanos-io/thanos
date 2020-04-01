# Contributing

This document explain the process of contributing to the Thanos project.

First of all please follow the [code of conduct](CODE_OF_CONDUCT.md) in all your interactions with the project.

## Thanos Philosophy

The philosophy of Thanos and our community is borrowing much from UNIX philosophy and the golang programming language.

* Each sub command should do one thing and do it well
  * eg. thanos query proxies incoming calls to known store API endpoints merging the result
* Write components that work together
  * e.g. blocks should be stored in native prometheus format
* Make it easy to read, write, and, run components
  * e.g. reduce complexity in system design and implementation

## Feedback / Issues

If you encounter any issue or you have an idea to improve, please:

* Search through Google and [existing open and closed GitHub Issues](https://github.com/thanos-io/thanos/issues) for the
answer first. If you find relevant topic, please comment on the issue.
* If not found, please add an issue to [GitHub issues](https://github.com/thanos-io/thanos/issues). Please provide
all relevant information as template suggest.
* If you have a quick question you might want to also ask on #thanos or #thanos-dev slack channel in the CNCF workspace.
We are recommending, using GitHub issues for issues and feedback, because GitHub issues are track-able.

If you encounter security vulnerability, please refer to [Reporting a Vulnerability process](SECURITY.md)

## Adding New Features / Components

When contributing not obvious change to Thanos repository, please first
discuss the change you wish to make via issue or slack, or any other
method with the owners of this repository before making a change.

Adding a large new feature or/and component to Thanos should be done by first creating a [proposal](docs/proposals) document outlining the design decisions of the change, motivations for the change, and any alternatives that might have been considered.

## Components Naming

Thanos is a distributed system composed with several services and CLI tools as listed [here](cmd/thanos).

When we refer to them as technical reference we use verb form: `store`, `compact`, `rule`, `query`. This includes:

* Code
* Metrics
* Commands
* Mixin examples: alerts, rules, dashboards
* Container names
* Flags and configuration
* Package names
* Log messages, traces

However, when speaking about those or explaining we use `actor` noun form: `store gateway, compactor, ruler, querier`. This includes areas like:

* Public communication
* Documentation
* Code comments

## Development

Following section explains various suggestions and procedures while development of Thanos.

### Prerequisites

* It is strongly recommended that you use OSX or popular Linux distributions systems e.g. Ubuntu, Redhat, or OpenSUSE for development.
* Go SDK 1.13.9 or newer installed.

### First steps

It's a key to get familiar with style guides and mechanics of Thanos, especially if your contribution touches more than on component of the Thanos distributed system.
We recommend:

* Reading [getting started docs](docs/getting-started.md) and getting through this. Alternatively https://katacoda.com/thanos.
* Familiarize yourself with our [coding style guides.](docs/contributing/coding-style-guide.md).
* Familiarize yourself with [Makefile](Makefile) commands like `format`, `build`, `proto`, `docker` and `test`. `make help` can print you some
available commands with explanation.

### Pull Request Process

1. Fork thanos-io/thanos.git and start development from your own fork. Here are sample steps to setup your development environment:

```console
$ mkdir -p $GOPATH/src/github.com/thanos-io
$ cd $GOPATH/src/github.com/thanos-io
$ git clone https://github.com/<your_github_id>/thanos.git
$ cd thanos
$ git remote add upstream https://github.com/thanos-io/thanos.git
$ git remote update
$ git merge upstream/master
$ make build
$ ./thanos -h
```

1. Keep PRs as small as possible. For each of your PR, you create one branch based on the latest master. Chain them if needed (base PR on other PRs). Here are sample steps you can follow. You can get more details about the workflow from [here](https://gist.github.com/Chaser324/ce0505fbed06b947d962).

```console
$ git checkout master
$ git remote update
$ git merge upstream/master
$ git checkout -b <your_branch_for_new_pr>
$ make build
$ <Iterate your development>
$ git push origin <your_branch_for_new_pr>
```

1. Add unit tests for new functionality. Add e2e tests if functionality is major.
1. If you don't have a live object store ready you can use `make test-local` command.

NOTE: This command skips live object storages by specifying environment variables, so the store specific tests will be run
against memory and filesystem object storages only. The CI runs `make test-ci` instead (uses GCS, AWS).

Not having these variables will produce auth errors against GCS, AWS, Azure, COS etc.

1. If your change affects users (adds or removes feature) consider adding the item to [CHANGELOG](CHANGELOG.md)
1. You may merge the Pull Request in once you have the sign-off of at least one developers with write access, or if you
   do not have permission to do that, you may request the second reviewer to merge it for you.
1. If you feel like your PR waits too long for a review, feel free to ping [`#thanos-prs`](https://slack.cncf.io/) channel on our slack for review!

### Dependency management

The Thanos project uses [Go modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) to manage dependencies on external packages. This requires a working Go environment with version 1.11 or greater and git installed.

To add or update a new dependency, use the `go get` command:

```bash
# Pick the latest tagged release.
go get example.com/some/module/pkg

# Pick a specific version.
go get example.com/some/module/pkg@vX.Y.Z
```

Tidy up the `go.mod` and `go.sum` files:

```bash
make deps
git add go.mod go.sum
git commit
```

You have to commit the changes to `go.mod` and `go.sum` before submitting the pull request.

### Advanced testing

At some point during development it is useful on top of unit or e2e tests, to run and play with Thanos components manually. While you
can run those components by crafting certain flags to some test setup there are already nice tools and scripts available.
Consider following methods:

* [quickstart.sh](https://github.com/thanos-io/thanos/blob/b08c0ea62abfe4dcf1400da0e37598f0cd8fa8cf/scripts/quickstart.sh): Script that spins
up example, simple setup. Do `make build` before running to build `thanos` binary first.
* `make test-e2e`: The e2e are covering most of the setups and actions we offer with Thanos. It's extremely easy to add `time.Sleep(10* time.Minutes)`
in certain point of tests (e.g for compactor [here](https://github.com/thanos-io/thanos/blob/8f492a9f073f819019dd9f044e346a1e1fa730bc/test/e2e/compact_test.go#L379)).
This way when you run `make test-e2e` which will sleep for some time, allowing you to connect to the setup manually. Use port that will be printed. For example:

```bash
querier-1: level=info name=querier-1 ts=2020-04-01T12:53:56.101029491Z caller=http.go:56 service=http/server component=query msg="listening for requests and metrics" address=:80
querier-1: level=info name=querier-1 ts=2020-04-01T12:53:56.101106805Z caller=intrumentation.go:48 msg="changing probe status" status=ready
querier-1: level=info name=querier-1 ts=2020-04-01T12:53:56.101290229Z caller=grpc.go:106 service=gRPC/server component=query msg="listening for StoreAPI gRPC" address=:9091
Ports for container: e2e_test_store_gateway-querier-1 Mapping: map[80:32825 9091:32824]
```

Means that HTTP endpoint will be available on `http://localhost:32825`. You can quickly craft your own test case with our framework as well!

NOTE: `make docker` has to work for `make test-e2e`. This currently does not work properly on MacOS.
