[Thanos](thanos.io) is a set of components that can be composed into a highly available metric system with unlimited storage capacity. It can be added seamlessly on top of existing Prometheus deployments.

This course uses docker containers with pre-built docker images.

In this tutorial, you will learn about :

* How to start uploading your Prometheus data seamlessly to cheap object storage thanks to Thanos sidecar.
* How to further query data in object storage thanks to Thanos Store Gateway: a metric browser that serves metric blocks stored in Object Store via *StoreAPI* gRPC API.
* How to query both fresh and older data in easy way through Thanos Querier.


All of this allows you to keep your metrics in cheap and reliable object storage, allowing virtually unlimited metric retention for Prometheus.

Let's jump in!
