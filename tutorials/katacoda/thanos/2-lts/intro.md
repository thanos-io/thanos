# Intro: Downsampling and unlimited metric retention for Prometheus

They say that [Thanos](thanos.io) is a set of components that can be composed into a highly available metric system with **unlimited storage capacity**
and that it can be added **seamlessly** on top of existing Prometheus deployments. 🤔🤔

In this course you can experience all of this yourself.

In this tutorial, you will learn about:

* How to start uploading your Prometheus data seamlessly to cheap object storage thanks to Thanos sidecar.
* How to further query your data in object storage thanks to Thanos Store Gateway.
* How to query both fresh and older data in easy way through Thanos Querier.

All of this allows you to keep your metrics in cheap and reliable object storage, allowing virtually unlimited metric retention for Prometheus.

> NOTE: This course uses docker containers with pre-built Thanos, Prometheus, and Minio Docker images available publicly.
> However, a similar scenario will work with any other deployment method like Kubernetes or systemd, etc.

### Prerequisites

Please complete first intro course about GlobalView before jumping into this one! 🤗

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?
Let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io

### Contributed by:

* Sonia Singla [@soniasingla](http://github.com/soniasingla)