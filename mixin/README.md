# thanos-mixin

> Note that everything is experimental and may change significantly at any time.
> Also it still has missing alert and dashboard definitions for certain components, e.g. rule and sidecar. Please feel free to contribute.

This directory contains extensible and customizable monitoring definitons for Thanos. [Grafana](http://grafana.com/) dashboards, and [Prometheus rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) combined with documentation and scripts to provide easy monitoring experience for Thanos.

You can find more about monitoring-mixins in [the design document](https://docs.google.com/document/d/1A9xvzwqnFVSOZ5fD3blKODXfsat5fg6ZhnKu9LK3lB4/edit#heading=h.gt9r2h2gklj3), and you could check out other examples like [Prometheus Mixin](https://github.com/prometheus/prometheus/tree/master/documentation/prometheus-mixin).

The content of this project is written in [jsonnet](http://jsonnet.org/). This project could both be described as a package as well as a library.

## Requirements

### jsonnet

The content of this project consists of a set of [jsonnet](http://jsonnet.org/) files making up a library to be consumed.

We recommend to use [go-jsonnet](https://github.com/google/go-jsonnet). It's an implementation of [Jsonnet](http://jsonnet.org/) in pure Go. It is feature complete but is not as heavily exercised as the [Jsonnet C++ implementation](https://github.com/google/jsonnet).

To install:

```shell
go get github.com/google/go-jsonnet/cmd/jsonnet
```

### jsonnet-bundler

`thanos-mixin` uses [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler#install) (the jsonnet package manager) to manage its dependencies.

We also recommend you to use `jsonnet-bundler` to install or update if you decide to use `thanos-mixin` as a dependency for your custom configurations.

To install:

```shell
go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
```

> An e.g. of how to install a given version of this library: `jb install github.com/thanos-io/thanos/mixin@master`.

## Use as a library

To use the `thanos-mixin` as a dependency, simply use the `jsonnet-bundler` install functionality:
```shell
$ mkdir thanos-mixin; cd thanos-mixin
$ jb init  # Creates the initial/empty `jsonnetfile.json`
# Install the thanos-mixin dependency
$ jb install github.com/thanos-io/thanos/mixin@master # Creates `vendor/` & `jsonnetfile.lock.json`, and fills in `jsonnetfile.json`
```

To update the `thanos-mixin` as a dependency, simply use the `jsonnet-bundler` update functionality:
```shell
$ jb update
```

#### Configure

This project is intended to be used as a library. You can extend and customize dashboards and alerting rules by creating for own generators, such as the generators ([alerts.jsonnet](alerts.jsonnet) and [dashboards.jsonnet](dashboards.jsonnet)) that are use to create [examples](../../examples). Default parameters are collected in [config.libsonnet](config.libsonnet), feel free to modify and generate your own definitons.

[embedmd]:# (config.libsonnet)
```libsonnet
{
  query+:: {
    jobPrefix: 'thanos-query',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sQuery' % $.dashboard.prefix,
  },
  store+:: {
    jobPrefix: 'thanos-store',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sStore' % $.dashboard.prefix,
  },
  receive+:: {
    jobPrefix: 'thanos-receive',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sReceive' % $.dashboard.prefix,
  },
  rule+:: {
    jobPrefix: 'thanos-rule',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sRule' % $.dashboard.prefix,
  },
  compact+:: {
    jobPrefix: 'thanos-compact',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sCompact' % $.dashboard.prefix,
  },
  sidecar+:: {
    jobPrefix: 'thanos-sidecar',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sSidecar' % $.dashboard.prefix,
  },
  bucket_replicate+:: {
    jobPrefix: 'thanos-bucket-replicate',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sBucketReplicate' % $.dashboard.prefix,
  },
  overview+:: {
    title: '%(prefix)sOverview' % $.dashboard.prefix,
  },
  dashboard+:: {
    prefix: 'Thanos / ',
    tags: ['thanos-mixin'],
    namespaceQuery: 'kube_pod_info',
  },
}
```

You can format your code using:
```shell
$ make jsonnet-format
```

## Examples

This project is intended to be used as a library. However, it also provides drop-in examples to monitor Thanos.

### Requirements

Although all the required dependencies are handled by `Makefile`, keep in mind that in addition the dependencies that are listed above we have following dependencies:

#### gojsontoyaml

`gojsontoyaml` is used to convert generated `json` definitons to `yaml`.

To install:
```shell
go get github.com/brancz/gojsontoyaml
```

### Generate

To generate examples after modifying, make sure `jsonnet` dependencies are installed.
```shell
$ make jsonnet-vendor
```

and then

```shell
$ make examples
```

Make action runs the jsonnet code, then reads each key of the generated json and uses that as the file name, and writes the value of that key to that file, and converts each json manifest to yaml.

> Make commands should handle dependecies for you.

### Test and validate

You validate your structural correctness of your Prometheus [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/) or [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) with:

```shell
$ make example-rules-lint
```

Check out [test.yaml](../../examples/alerts/tests.yaml) to add/modify tests for the mixin. To learn more about how to write test for Prometheus, check out [official documentation](https://www.prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).

You test alerts with:

```shell
$ make alerts-test
```

---
