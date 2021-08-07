# Contributing

This document explain the process of contributing to the Thanos project.

First of all please follow the [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md) in all your interactions with the project.

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

* Search through Google and [existing open and closed GitHub Issues](https://github.com/thanos-io/thanos/issues) for the answer first. If you find relevant topic, please comment on the issue.
* If not found, please add an issue to [GitHub issues](https://github.com/thanos-io/thanos/issues). Please provide all relevant information as template suggest.
* If you have a quick question you might want to also ask on #thanos or #thanos-dev slack channel in the CNCF workspace. We are recommending, using GitHub issues for issues and feedback, because GitHub issues are track-able.

If you encounter security vulnerability, please refer to [Reporting a Vulnerability process](SECURITY.md)

## Adding New Features / Components

When contributing not obvious change to Thanos repository, please first discuss the change you wish to make via issue or slack, or any other method with the owners of this repository before making a change.

Adding a large new feature or/and component to Thanos should be done by first creating a [proposal](docs/proposals) document outlining the design decisions of the change, motivations for the change, and any alternatives that might have been considered.

## General Naming

In the code and documentation prefer non-offensive terminology, for example:

* `allowlist` / `denylist` (instead of `whitelist` / `blacklist`)
* `primary` / `replica` (instead of `master` / `slave`)
* `openbox` / `closedbox` (instead of `whitebox` / `blackbox`)

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

The following section explains various suggestions and procedures to note during development of Thanos.

### Prerequisites

* It is strongly recommended that you use Linux distributions systems or OSX for development.
* Go 1.13.9 or newer installed.
* For React UI, you will need a working NodeJS environment and the Yarn package manager to compile the Web UI assets

### First Steps

It's key to get familiarized with style guide and mechanics of Thanos, especially if your contribution touches more than one component of the Thanos distributed system. We recommend:

* Reading the [getting started docs](docs/getting-started.md) and working through them, or alternatively working through the [Thanos tutorial](https://katacoda.com/thanos).
* Familiarizing yourself with our [coding style guidelines.](docs/contributing/coding-style-guide.md).
* Familiarizing yourself with the [Makefile](Makefile) commands, for example `format`, `build`, `proto`, `docker` and `test`. `make help` will print most of available commands with details.

### Pull Request Process

1. Fork thanos-io/thanos.git and start development from your own fork. Here are sample steps to setup your development environment:

```console
$ GOPATH=$(go env GOPATH)
$ mkdir -p $GOPATH/src/github.com/thanos-io
$ cd $GOPATH/src/github.com/thanos-io
$ git clone https://github.com/<your_github_id>/thanos.git
$ cd thanos
$ git remote add upstream https://github.com/thanos-io/thanos.git
$ git remote update
$ git merge upstream/master
$ make build
$ export PATH=$PATH:$GOPATH/bin
$ thanos -h
```

1. Signing your work: DCO (Developer Certificate of Origin) Process.

By contributing to this project you agree to the [Developer Certificate of Origin](https://developercertificate.org/)(DCO). This document was created by the Linux Kernel community and is a simple statement that you, as a contributor, have the legal right to make the contribution.

To signoff, you need to add `Signed-off-by: Your Name <your email id>` at the end of your commit messages. You can do this using [`git commit -s`](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt--s). For example:

```
$ git commit -s -m 'This is my commit message'
```

You can also alias `commit` as `commit -s` in your `~/.gitconfig` to signoff all your future commits.

If you have authored an unsigned commit, you can update it using `git commit --amend --signoff`. If you've pushed your changes to GitHub already you'll need to force push your branch after this with `git push -f`.

1. Keep PRs as small as possible. For each of your PRs, you create a new branch based on the latest master. Chain them if needed (base one PR on other PRs). You can read more details about the workflow from [here](https://gist.github.com/Chaser324/ce0505fbed06b947d962).

```console
$ git checkout master
$ git remote update
$ git merge upstream/master
$ git checkout -b <your_branch_for_new_pr>
$ make build
$ <Iterate your development>
$ git push origin <your_branch_for_new_pr>
```

1. Add unit tests for new functionality. Add e2e tests for major changes to functionality.
2. If you don't have a live object store ready, you can use the `make test-local` command.

NOTE: this command skips tests against live object storage systems by specifying environment variables; this causes the store-specific tests to be run against memory and filesystem object storage types only. The CI tests run uses GCS, AWS and Swift.

Not specifying these variables will result in auth errors against GCS, AWS, Azure, COS etc.

1. If your change affects users (adds or removes feature) consider adding the item to the [CHANGELOG](CHANGELOG.md).
2. You may merge the Pull Request once you have the sign-off of at least one developer with write access, or if you do not have permission to do that, you may request the second reviewer to merge it for you.
3. If you feel like your PR is waiting too long for a review, feel free to ping the [`#thanos-prs`](https://slack.cncf.io/) channel on our slack for a review!

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

At some point during development it is useful, in addition to running unit or e2e tests, to run and play with Thanos components manually. While you can run any component manually by crafting specific flags for a test setup, there are already some nice tools and scripts available. Consider the following methods:

* [quickstart.sh](https://github.com/thanos-io/thanos/blob/b08c0ea62abfe4dcf1400da0e37598f0cd8fa8cf/scripts/quickstart.sh): this script spins up a simple example setup. Do `make build` before running the script to build the `thanos` binary first.
* `make test-e2e`: the e2e tests cover most of the setups and functionality Thanos offers. It's extremely easy to add `time.Sleep(10* time.Minutes)` at certain points in the tests (e.g for compactor [here](https://github.com/thanos-io/thanos/blob/8f492a9f073f819019dd9f044e346a1e1fa730bc/test/e2e/compact_test.go#L379)). This way when you run `make test-e2e`, the test will sleep for some time, allowing you to connect to the setup manually using the port printed in the logs. For example:

```bash
querier-1: level=info name=querier-1 ts=2020-04-01T12:53:56.101029491Z caller=http.go:56 service=http/server component=query msg="listening for requests and metrics" address=:80
querier-1: level=info name=querier-1 ts=2020-04-01T12:53:56.101106805Z caller=intrumentation.go:48 msg="changing probe status" status=ready
querier-1: level=info name=querier-1 ts=2020-04-01T12:53:56.101290229Z caller=grpc.go:106 service=gRPC/server component=query msg="listening for StoreAPI gRPC" address=:9091
Ports for container: e2e_test_store_gateway-querier-1 Mapping: map[80:32825 9091:32824]
```

This output indicates that the HTTP (`80`) endpoint will be available on `http://localhost:32825`. You can quickly craft your own test case with our framework as well!

NOTE: `make docker` has to work in order for `make test-e2e` to run. This currently might not work properly on macOS.
