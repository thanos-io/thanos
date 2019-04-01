<p align="center"><img src="docs/img/Thanos-logo_fullmedium.png" alt="Thanos Logo"></p>

[![CircleCI](https://circleci.com/gh/improbable-eng/thanos.svg?style=svg)](https://circleci.com/gh/improbable-eng/thanos)
[![Go Report Card](https://goreportcard.com/badge/github.com/improbable-eng/thanos)](https://goreportcard.com/report/github.com/improbable-eng/thanos)
[![GoDoc](https://godoc.org/github.com/improbable-eng/thanos?status.svg)](https://godoc.org/github.com/improbable-eng/thanos)
[![Slack](https://img.shields.io/badge/join%20slack-%23thanos-brightgreen.svg)](https://join.slack.com/t/improbable-eng/shared_invite/enQtMzQ1ODcyMzQ5MjM4LWY5ZWZmNGM2ODc5MmViNmQ3ZTA3ZTY3NzQwOTBlMTkzZmIxZTIxODk0OWU3YjZhNWVlNDU3MDlkZGViZjhkMjc)
[![Docker Pulls](https://img.shields.io/docker/pulls/improbable/thanos.svg?maxAge=604800)](https://hub.docker.com/r/improbable/thanos/)

## Overview

Thanos is a set of components that can be composed into a highly available metric
system with unlimited storage capacity, which can be added seamlessly on top of existing
Prometheus deployments.

Thanos leverages the Prometheus 2.0 storage format to cost-efficiently store historical metric
data in any object storage while retaining fast query latencies. Additionally, it provides
a global query view across all Prometheus installations and can merge data from Prometheus
HA pairs on the fly.

Concretely the aims of the project are:

1. Global query view of metrics.
1. Unlimited retention of metrics.
1. High availability of components, including Prometheus.

## Architecture Overview

![architecture_overview](docs/img/arch.jpg)

## Getting Started

* **[Getting Started](docs/getting_started.md)**
* [Design](docs/design.md)
* [Prom Meetup Slides](https://www.slideshare.net/BartomiejPotka/thanos-global-durable-prometheus-monitoring)
* [Introduction blog post](https://improbable.io/games/blog/thanos-prometheus-at-scale)
* [Benchmarks](https://github.com/improbable-eng/thanos/tree/master/benchmark)
* [Proposals](docs/proposals)

## Features

* Global querying view across all connected Prometheus servers
* Deduplication and merging of metrics collected from Prometheus HA pairs
* Seamless integration with existing Prometheus setups
* Any object storage as its only, optional dependency
* Downsampling historical data for massive query speedup
* Cross-cluster federation
* Fault-tolerant query routing
* Simple gRPC "Store API" for unified data access across all metric data
* Easy integration points for custom metric providers

## Thanos Philosophy

The philosophy of Thanos and our community is borrowing much from UNIX philosophy and the golang programming language.

* Each sub command should do one thing and do it well
  * eg. thanos query proxies incoming calls to known store API endpoints merging the result
* Write components that work together
  * e.g. blocks should be stored in native prometheus format
* Make it easy to read, write, and, run components
  * e.g. reduce complexity in system design and implementation

## Contributing

Contributions are very welcome! See our [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## Community

Thanos is an open source project and we welcome new contributers and members
of the community. Here are ways to get in touch with the community:

* Slack: [#thanos](https://join.slack.com/t/improbable-eng/shared_invite/enQtMzQ1ODcyMzQ5MjM4LWY5ZWZmNGM2ODc5MmViNmQ3ZTA3ZTY3NzQwOTBlMTkzZmIxZTIxODk0OWU3YjZhNWVlNDU3MDlkZGViZjhkMjc)
* Issue Tracker: [GitHub Issues](https://github.com/improbable-eng/thanos/issues)
