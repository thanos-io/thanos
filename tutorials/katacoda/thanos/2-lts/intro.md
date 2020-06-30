[Thanos](thanos.io) is a set of components that can be composed into a highly available metric system with unlimited storage capacity. It can be added seamlessly on top of existing Prometheus deployments.

Thanos allows users to aggregate Prometheus data natively by directly querying the Prometheus API, efficiently compact it, and most importantly, de-duplicate data.

Thanos will work in cloud native environments like Kubernetes as well as more traditional ones. However, this course uses docker containers which will allow us to use pre-built docker images.

In this tutorial, you will learn about :

* Thanos Store Gateway : a metric browser that translate blocks into StoreAPI gRPC API.
* Querying multiple Prometheus instances from single Prometheus API endpoint.

Let's jump in! 🤓

https://thanos.io
