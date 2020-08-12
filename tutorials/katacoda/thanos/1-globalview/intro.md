# Welcome to the Thanos introduction!

[Thanos](https://thanos.io) is a set of components that can be composed into a highly available metric system with unlimited storage capacity.
It can be added seamlessly on top of existing Prometheus deployments.

Thanos provides a global query view, data backup, and historical data access as its core features.
All three features can be run independently of each other. This allows you to have a subset of Thanos features ready for
immediate benefit or testing, while also making it flexible for gradual adoption in more complex environments.

Thanos will work in cloud native environments like Kubernetes as well as more traditional ones. However, this course uses docker
containers which will allow us to use pre-built docker images available [here](https://quay.io/repository/thanos/thanos)

This tutorial will take us from transforming vanilla Prometheus to basic Thanos deployment enabling:

* Reliable querying multiple Prometheus instances from single [Prometheus API endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#expression-queries).
* Seamless handling of Highly Available Prometheus (multiple replicas)

Let's jump in! 🤓

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?
Let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io

### Contributed by:

* Bartek [@bwplotka](https://bwplotka.dev/)