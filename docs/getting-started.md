# Getting started

Thanos provides a global query view, high availability, data backup with historical, cheap data access as its core features in a single binary.

Those features can be deployed independently of each other. This allows you to have a subset of Thanos features ready for immediate benefit or testing, while also making it flexible for gradual roll outs in more complex environments.

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

Main should be stable and usable. Every commit to main builds docker image named `main-<data>-<sha>` in [quay.io/thanos/thanos](https://quay.io/repository/thanos/thanos) and [thanosio/thanos dockerhub (mirror)](https://hub.docker.com/r/thanosio/thanos)

We also perform minor releases every 6 weeks.

During that, we build tarballs for major platforms and release docker images.

See [release process docs](release-process.md) for details.

## Building from source:

Thanos is built purely in [Golang](https://go.dev/), thus allowing to run Thanos on various x64 operating systems.

Thanos can **not** be downloaded nor installed via the `go get` or `go install` methods. Starting in Go 1.17, installing executables with `go get` is deprecated. `go install` may be used instead. However, in order to avoid ambiguity, when go install is used with a version suffix, all arguments must refer to main packages in the same module at the same version. If that module has a `go.mod` file, it must not contain directives like *replace* or *exclude* that would cause it to be interpreted differently if it were the main module.

Thanos uses the directive *replace*. The reason is to provide a way to unblock ourselves promptly while also being flexible in the packages that we (re)use. Support for `go install` is not likely at this point.

If you want to build Thanos from source you would need a working installation of the Go 1.18+ [toolchain](https://github.com/golang/tools) (`GOPATH`, `PATH=${GOPATH}/bin:${PATH}`). Next one should make a clone of our repository:

```
git clone git@github.com:thanos-io/thanos.git
```

When you have access to the source code locally, we have prepared a `Makefile`. Invoke this by using `make` in your CLI. For example `make help` will list all options. For building Thanos one could use `make build`

The `thanos` binary should now be in your project folder and is the only thing required to deploy any of its components.

## Contributing

Contributions are very welcome! See our [CONTRIBUTING.md](../CONTRIBUTING.md) for more information.

## Community

Thanos is an open source project and we value and welcome new contributors and members of the community. Here are ways to get in touch with the community:

* Slack: [#thanos](https://slack.cncf.io/)
* Issue Tracker: [GitHub Issues](https://github.com/thanos-io/thanos/issues)

## Maintainers

See [MAINTAINERS.md](../MAINTAINERS.md).

## Community Thanos Kubernetes Applications

Thanos is **not** tied to Kubernetes. However, Kubernetes, Thanos and Prometheus are part of the CNCF so the most popular applications are on top of Kubernetes.

Our friendly community maintains a few different ways of installing Thanos on Kubernetes. See those below:

* [prometheus-operator](https://github.com/coreos/prometheus-operator): Prometheus operator has support for deploying Prometheus with Thanos
* [kube-thanos](https://github.com/thanos-io/kube-thanos): Jsonnet based Kubernetes templates.
* [Community Helm charts](https://artifacthub.io/packages/search?ts_query_web=thanos)

If you want to add yourself to this list, let us know!

## Deploying Thanos

* Detailed, free, in-browser interactive tutorial [as Killercoda Thanos Course](https://killercoda.com/thanos/)
* [Quick Tutorial](quick-tutorial.md) on Thanos website.

## Operating

See up to date [jsonnet mixins](https://github.com/thanos-io/thanos/tree/main/mixin/README.md) We also have example Grafana dashboards [here](https://github.com/thanos-io/thanos/blob/main/examples/dashboards/dashboards.md) and some [alerts](https://github.com/thanos-io/thanos/blob/main/examples/alerts/alerts.md) to get you started.

## Talks

* 2023
  * [Planetscale monitoring: Handling billions of active series with Prometheus and Thanos](https://www.youtube.com/watch?v=Or8r46fSaOg)
  * [Taming the Tsunami: low latency ingestion of push-based metrics in Prometheus](https://www.youtube.com/watch?v=W81x1j765hc)

* 2022
  * [Story of Correlation: Integrating Thanos Metrics with Observability Signals](https://www.youtube.com/watch?v=rWFb01GW0mQ)
  * [Running the Observability As a Service For Your Teams With Thanos](https://www.youtube.com/watch?v=I4Mfyfd_4M8)
  * [Monitoring multiple Kubernetes Clusters with Thanos](https://www.youtube.com/watch?v=V4v-c0VeqLw)
  * [Thanos: Scaling Prometheus 101](https://www.youtube.com/watch?v=iN6DR28gAyQ)
  * [MaaS for the Masses: Build Your Monitoring-as-a-Service Solution With Prometheus](https://www.youtube.com/watch?v=EFPPic9dBS4)

* 2021
  * [Adopting Thanos gradually across all of LastPass infrastructures](https://www.youtube.com/watch?v=Ddq8m04594A)
  * [Using Thanos to gain a unified way to query over multiple clusters](https://www.youtube.com/watch?v=yefffBLuVh0)
  * [Thanos: Easier Than Ever to Scale Prometheus and Make It Highly Available](https://www.youtube.com/watch?v=mtwwUqeIHAw)

* 2020
  * [Absorbing Thanos Infinite Powers for Multi-Cluster Telemetry](https://www.youtube.com/watch?v=6Nx2BFyr7qQ)
  * [Turn It Up to a Million: Ingesting Millions of Metrics with Thanos Receive](https://www.youtube.com/watch?v=5MJqdJq41Ms)
  * [Thanos: Cheap, Simple and Scalable Prometheus](https://www.youtube.com/watch?v=Wroo1n5GWwg)
  * [Thanos: Prometheus at Scale!](https://www.youtube.com/watch?v=q9j8vpgFkoY)
  * [Introduction to Thanos](https://www.youtube.com/watch?v=j4TAGO019HU)
  * [Using Thanos as a long term storage for your Prometheus metrics](https://www.youtube.com/watch?v=cedzqLgRgaM)

* 2019
  * [FOSDEM + demo](https://fosdem.org/2019/schedule/event/thanos_transforming_prometheus_to_a_global_scale_in_a_seven_simple_steps/)
  * [Alibaba Cloud user story](https://www.youtube.com/watch?v=ZS6zMksfipc)
  * [CloudNative Warsaw Slides](https://docs.google.com/presentation/d/1cKpbJY3jIAtr03M-zcNujwBA38_LDj7NqE4LjNfvglE/edit?usp=sharing)
  * [CloudNative Deep Dive](https://www.youtube.com/watch?v=qQN0N14HXPM)
  * [CloudNative Intro](https://www.youtube.com/watch?v=m0JgWlTc60Q)
  * [Prometheus in Practice: HA with Thanos](https://www.slideshare.net/ThomasRiley45/prometheus-in-practice-high-availability-with-thanos-devopsdays-edinburgh-2019)

* 2018
  * [Very first Prometheus Meetup Slides](https://www.slideshare.net/BartomiejPotka/thanos-global-durable-prometheus-monitoring)

## Blog posts

* 2023:
  * [Thanos Ruler and Prometheus Rules — a match made in heaven.](https://medium.com/@helia.barroso/thanos-ruler-and-prometheus-rules-a-match-made-in-heaven-a4f08f2399ac)

* 2022:
  * [Thanos at Medallia: A Hybrid Architecture Scaled to Support 1 Billion+ Series Across 40+ Data Centers](https://thanos.io/blog/2022-09-08-thanos-at-medallia/)
  * [Deploy Thanos Receive with native OCI Object Storage on Oracle Kubernetes Engine](https://medium.com/@lmukadam/deploy-thanos-receive-with-native-oci-object-storage-on-kubernetes-829326ea0bc6)
  * [Leveraging Consul for Thanos Query Discovery](https://nicolastakashi.medium.com/leveraging-consul-for-thanos-query-discovery-34212d496c88)

* 2021:
  * [Adopting Thanos at LastPass](https://krisztianfekete.org/adopting-thanos-at-lastpass/)

* 2020:
  * [Banzai Cloud user story](https://outshift.cisco.com/blog/multi-cluster-monitoring)
  * [Monitoring the Beat microservices: A tale of evolution](https://build.thebeat.co/monitoring-the-beat-microservices-a-tale-of-evolution-4e246882606e)

* 2019:
  * [Metric monitoring architecture](https://improbable.io/blog/thanos-architecture-at-improbable)
  * [Red Hat user story](https://blog.openshift.com/federated-prometheus-with-thanos-receive/)
  * [HelloFresh blog posts part 1](https://engineering.hellofresh.com/monitoring-at-hellofresh-part-1-architecture-677b4bd6b728)
  * [HelloFresh blog posts part 2](https://engineering.hellofresh.com/monitoring-at-hellofresh-part-2-operating-the-monitoring-system-8175cd939c1d)
  * [Thanos deployment](https://www.metricfire.com/blog/ha-kubernetes-monitoring-using-prometheus-and-thanos)
  * [Taboola user story](https://blog.taboola.com/monitoring-and-metering-scale/)
  * [Thanos via Prometheus Operator](https://kkc.github.io/2019/02/10/prometheus-operator-with-thanos/)

* 2018:
  * [Introduction blog post](https://improbable.io/blog/thanos-prometheus-at-scale)
  * [Monzo user story](https://monzo.com/blog/2018/07/27/how-we-monitor-monzo)
  * [uSwitch user story](https://medium.com/uswitch-labs/making-prometheus-more-awesome-with-thanos-fbec8c6c28ad)
  * [Thanos usage](https://www.infracloud.io/blogs/thanos-ha-scalable-prometheus/)

## Integrations

See [Integrations page](integrations.md).

## Testing Thanos on Single Host

We don't recommend running Thanos on a single node on production. Thanos is designed and built to run as a distributed system. Vanilla Prometheus might be totally enough for small setups.

However, in case you want to play and run Thanos components on a single node, we recommend following the port layout:

| Component      | Interface               | Port  |
|----------------|-------------------------|-------|
| Sidecar        | gRPC                    | 10901 |
| Sidecar        | HTTP                    | 10902 |
| Query          | gRPC                    | 10903 |
| Query          | HTTP                    | 10904 |
| Store          | gRPC                    | 10905 |
| Store          | HTTP                    | 10906 |
| Receive        | gRPC (store API)        | 10907 |
| Receive        | HTTP (remote write API) | 10908 |
| Receive        | HTTP                    | 10909 |
| Rule           | gRPC                    | 10910 |
| Rule           | HTTP                    | 10911 |
| Compact        | HTTP                    | 10912 |
| Query Frontend | HTTP                    | 10913 |

You can see example one-node setup [here](../scripts/quickstart.sh).
