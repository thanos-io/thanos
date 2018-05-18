<p align="center"><img src="docs/img/Thanos-logo_fullmedium.png" alt="Thanos Logo"></p>

[![CircleCI](https://circleci.com/gh/improbable-eng/thanos.svg?style=svg)](https://circleci.com/gh/improbable-eng/thanos)
[![Go Report Card](https://goreportcard.com/badge/github.com/improbable-eng/thanos)](https://goreportcard.com/report/github.com/improbable-eng/thanos)
[![Slack](docs/img/slack.png)](https://join.slack.com/t/improbable-eng/shared_invite/enQtMzQ1ODcyMzQ5MjM4LWY5ZWZmNGM2ODc5MmViNmQ3ZTA3ZTY3NzQwOTBlMTkzZmIxZTIxODk0OWU3YjZhNWVlNDU3MDlkZGViZjhkMjc)

## Overview

Thanos is a set of components that can be composed into a highly available
metric system with unlimited storage capacity. It can be added seamlessly on
top of existing Prometheus deployments and leverages the Prometheus 2.0
storage format to cost-efficiently store historical metric data in any object
storage while retaining fast query latencies. Additionally, it provides
a global query view across all Prometheus installations and can merge
data from Prometheus HA pairs on the fly.

* **[Getting Started](docs/getting_started.md)**
* [Design](docs/design.md)
* [Prom Meetup Slides](https://www.slideshare.net/BartomiejPotka/thanos-global-durable-prometheus-monitoring)
* [Introduction blog post](https://improbable.io/games/blog/thanos-prometheus-at-scale)

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

## Contributing

Contributions are very welcome!

## Community

Thanos is an open source project and we welcome new contributers and members 
of the community. Here are ways to get in touch with the community:

* Slack: [#thanos](https://join.slack.com/t/improbable-eng/shared_invite/enQtMzQ1ODcyMzQ5MjM4LWY5ZWZmNGM2ODc5MmViNmQ3ZTA3ZTY3NzQwOTBlMTkzZmIxZTIxODk0OWU3YjZhNWVlNDU3MDlkZGViZjhkMjc)
* Twitter: [@improbableio](https://twitter.com/Improbableio)
* Issue Tracker: [GitHub Issues](https://github.com/improbable-eng/thanos/issues)
