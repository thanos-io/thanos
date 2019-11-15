---
title: Getting Started
type: docs
menu: thanos
weight: 1
slug: /getting-started.md
---

# Getting started

Thanos provides a global query view, high availability, data backup with historical, cheap data access as its core features in a single binary. 

Those features can be deployed independently of each other. This allows you to have a subset of Thanos features ready 
for immediate benefit or testing, while also making it flexible for gradual roll outs in more complex environments. 

In this quick-start guide, we will explain:

* How to ask questions, build and contribute to Thanos.
* A few common ways of deploying Thanos.
* Links for further reading.

Thanos will work in cloud native environments as well as more traditional ones. Some users run Thanos in Kubernetes while others on the bare metal.

## Dependencies

Thanos aims for a simple deployment and maintenance model. The only dependencies are:

* One or more [Prometheus](https://prometheus.io) v2.2.1+ installations with persistent disk.
* Optional object storage
  * Thanos is able to use [many different storage providers](storage.md), with the ability to add more providers as necessary.

## Get Thanos!

You can find the latest Thanos release [here](https://github.com/thanos-io/thanos/releases).

Master should be stable and usable. Every commit to master builds docker image named `master-<data>-<sha>` in 
[quay.io/thanos/thanos](https://quay.io/repository/thanos/thanos) and [thanosio/thanos dockerhub (mirror)](https://hub.docker.com/r/thanosio/thanos)

We also perform minor releases every 6 weeks.

During that, we build tarballs for major platforms and release docker images.

See [release process docs](release-process.md) for details.

## Building from source:

Thanos is built purely in [Golang](https://golang.org/), thus allowing to run Thanos on various x64 operating systems. 

If you want to build Thanos from source you would need a working installation of the Go 1.12+ [toolchain](https://github.com/golang/tools) (`GOPATH`, `PATH=${GOPATH}/bin:${PATH}`).

Thanos can be downloaded and built by running:

```bash
go get github.com/thanos-io/thanos/cmd/thanos
```

The `thanos` binary should now be in your `$PATH` and is the only thing required to deploy any of its components.

## Contributing

Contributions are very welcome! See our [CONTRIBUTING.md](/CONTRIBUTING.md) for more information.

## Community

Thanos is an open source project and we value and welcome new contributors and members
of the community. Here are ways to get in touch with the community:

* Slack: [#thanos](https://slack.cncf.io/)
* Issue Tracker: [GitHub Issues](https://github.com/thanos-io/thanos/issues)

## Maintainers

See [MAINTAINERS.md](/MAINTAINERS.md)

## Community Thanos Kubernetes Applications

Thanos is **not** tied to Kubernetes. However, Kubernetes, Thanos and Prometheus are part of the CNCF so the most popular applications are on top of Kubernetes.

Our friendly community maintains a few different ways of installing Thanos on Kubernetes. See those below:

* [prometheus-operator](https://github.com/coreos/prometheus-operator): Prometheus operator has support for deploying Prometheus with Thanos
* [kube-thanos](https://github.com/thanos-io/kube-thanos): Jsonnet based Kubernetes templates.
* [Community Helm charts](https://hub.helm.sh/charts?q=thanos)

If you want to add yourself to this list, let us know!

## Deploying Thanos

* [WIP] Detailed, free, in-browser interactive tutorial [as Katacoda Thanos Course](https://katacoda.com/bwplotka/courses/thanos)
* [Quick Tutorial](./quick-tutorial.md) on Thanos website.

## Operating

See up to date [jsonnet mixins](https://github.com/thanos-io/kube-thanos/tree/master/jsonnet/thanos-mixin)
We also have example Grafana dashboards [here](/examples/grafana/monitoring.md) and some [alerts](/examples/alerts/alerts.md) to get you started.

## Talks

* 02.2018: [Very first Prometheus Meetup Slides](https://www.slideshare.net/BartomiejPotka/thanos-global-durable-prometheus-monitoring)
* 02.2019: [FOSDEM + demo](https://fosdem.org/2019/schedule/event/thanos_transforming_prometheus_to_a_global_scale_in_a_seven_simple_steps/)
* 09.2019: [CloudNative Warsaw Slides](https://docs.google.com/presentation/d/1cKpbJY3jIAtr03M-zcNujwBA38_LDj7NqE4LjNfvglE/edit?usp=sharing)

## Blog posts

* 2018: [Introduction blog post](https://improbable.io/games/blog/thanos-prometheus-at-scale)
* 2019: [Metric monitoring architecture](https://improbable.io/blog/thanos-architecture-at-improbable)

## Integrations

See [Integrations page](./integrations.md)

## Testing Thanos on Single Host

We don't recommend running Thanos on a single node on production.
Thanos is designed and built to run as a distributed system.
Vanilla Prometheus might be totally enough for small setups.

However, in case you want to play and run Thanos components
on a single node, we recommend following the port layout:

| Component | Interface               | Port  |
| --------- | ----------------------- | ----- |
| Sidecar   | gRPC                    | 10901 |
| Sidecar   | HTTP                    | 10902 |
| Query     | gRPC                    | 10903 |
| Query     | HTTP                    | 10904 |
| Store     | gRPC                    | 10905 |
| Store     | HTTP                    | 10906 |
| Receive   | gRPC (store API)        | 10907 |
| Receive   | HTTP (remote write API) | 10908 |
| Receive   | HTTP                    | 10909 |
| Rule      | gRPC                    | 10910 |
| Rule      | HTTP                    | 10911 |
| Compact   | HTTP                    | 10912 |

You can see example one-node setup [here](/scripts/quickstart.sh)
