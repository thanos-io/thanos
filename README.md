# Thanos

[![Build Status](https://travis-ci.org/improbable-eng/thanos.svg?branch=master)](https://travis-ci.org/improbable-eng/thanos) [![Go Report Card](https://goreportcard.com/badge/github.com/improbable-eng/thanos)](https://goreportcard.com/report/github.com/improbable-eng/thanos)

Thanos is a set of clustered components that implement a scalable metric storage on top of the Prometheus 2.0 storage engine.

* [Design](/docs/design.md)
* [Component docs](/docs/components)

Currently, Thanos only supports Google Cloud Storage.

Contributions to add more backends are welcome and only require implementing a [simple interface](https://github.com/improbable-eng/thanos/blob/86651e9402115c6476b35ac2bb3d6003c9ec9956/pkg/objstore/objstore.go#L15-L40).
